#!/usr/bin/python3
import logging, sys, time, os
from Socket_Singleton import Socket_Singleton
from asyncua import Client
from asyncua.crypto import security_policies
from asyncua.common import Node
import asyncio

'''
PyInstaller ToDo:
pyinstaller --onefile --no-console --icon "niles_ori.ico" --name shutdown_service Main.py
'''


class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    """
    def __init__(self):
        self.shutdown = False
        self.dict_reaction = {}
        self.dict_states = {}

    def datachange_notification(self, node:Node, val, data):
        if self.dict_reaction[node] == 'info':
            logging.info(f'Device state changed. New Device state is: {self.dict_states[val]}')
        elif self.dict_reaction[node] == 'shutdown':
            if val:
                logging.info(f'Shutdown event. Shutting system down.')
                os.system('shutdown /s /f /t 5')
        else:
            logging.info(f'unmanaged datachange event from: {node} value: {val}')

    def event_notification(self, event):
        logging.info(f'Python: New event {event}')



class OPCHandler:

    '''
    1. OPC UA Server "Zertifikate autom. akzeptieren" muss aktiv sein sonst Exception "BadSecurityChecksFailed"
    2. Wenn Security != None
    security übersetzen in entsprechenden Aufruf aus asyncua.crypto.security_policies
    cert_path + key als raw string
    z.B.:
    cert = f"certificates/peer-certificate-example.der"
    private_key = f"certificates/peer-private-key-example.pem"
    '''

    def __init__(self, endpoint,user,password,security=None,cert_path=None,private_key=None):
        self.endpoint = endpoint
        self.user = user
        self.password = password
        self.cert = cert_path
        self.client = Client(endpoint,timeout=5)
        self.client.set_user(user)
        self.client.set_password(password)
        self.renew_channel = False

        if security is not None:
            self.client.set_security(
                getattr(security_policies, 'SecurityPolicy' + security),
                cert_path,
                private_key)
        logging.info("Init successfully for endpoint: %r",endpoint)
        return


    async def __aenter__(self):
        logging.info("Trying to connect to endpoint: %r",self.endpoint)

        '''
        #Connect and Create Session
        '''
        try:
            await self.client.connect_socket()
            await self.client.send_hello()
            await self.client.open_secure_channel(renew=self.renew_channel)
            await self.client.create_session()
            await self.client.activate_session(username=self.user,password=self.password,certificate=self.cert)

        except:
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')
            logging.error(f'Error on Connection Initialization. Closing Session.')
            await self.client.close_session()
            await self.client.disconnect_socket()
            sys.exit(202)

        finally:
            self.renew_channel = True
            return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logging.info(f'Disconnecting from Server')
        try:
            await self.client.close_session()
        finally:
            await self.client.disconnect_socket()




def init_logging():
    log_format = f"%(asctime)s [%(processName)s] [%(name)s] [%(levelname)s]  %(message)s"
    log_level = logging.INFO
    # noinspection PyArgumentList
    logging.basicConfig(
        format = log_format,
        level = log_level,
        handlers = [
            logging.FileHandler(filename='shutdown_service.log',mode='w',encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def init():
    Socket_Singleton(address="127.0.0.1", port=1337, timeout=0, client=True, strict=True)
    init_logging()


async def main():
    endpoint = 'opc.tcp://192.168.214.249:4840'
    user = 'admin'
    password = r'Sunrise2000!'
    async with OPCHandler(endpoint,user,password) as client:
        logging.info(f"Successfully connected to Server {endpoint}")

        '''
        #get namespaces + root node
        '''
        ns_root = await client.get_namespace_index('http://opcfoundation.org/UA/')
        ns_dev = await client.get_namespace_index('http://opcfoundation.org/UA/DI/')
        namespace = await client.get_namespace_index('SitopProvider')
        root = client.get_root_node()


        '''
        #get Buffer Alarm
        '''
        alarm_lst = []
        alarm_lst.append(f'{ns_root}:Objects')
        alarm_lst.append(f'{ns_dev}:DeviceSet')
        alarm_lst.append(f'{namespace}:PSU8600')
        alarm_lst.append(f'{namespace}:Alarm')
        alarm_lst.append(f'{namespace}:Alarm_MR_SystemStateBuffering')

        alarm_node = await root.get_child(alarm_lst)
        alarm_node_val = await alarm_node.get_value()
        logging.info(f'current state of Buffering Alarm: {alarm_node_val}')



        '''
        #get Device State
        '''
        buffer_lst = []
        buffer_lst.append(f'{ns_root}:Objects')
        buffer_lst.append(f'{ns_dev}:DeviceSet')
        buffer_lst.append(f'{namespace}:PSU8600')
        buffer_lst.append(f'{namespace}:ActualState')
        buffer_lst.append(f'{namespace}:DeviceState')

        buffer_node = await root.get_child(buffer_lst)
        buffer_node_val = await buffer_node.get_value()
        logging.info(f'current state of Buffer Device: {buffer_node_val}')

        buffer_dict_lst = buffer_lst
        buffer_dict_lst.append(f'{ns_root}:EnumStrings')
        buffer_dict_node = await root.get_child(buffer_dict_lst)
        buffer_dict_val = await buffer_dict_node.get_value()
        buffer_dict={}
        for i, val in enumerate(buffer_dict_val):
            buffer_dict[i] = val.Text

        '''
        get server state
        '''
        server_lst = []
        server_lst.append(f'{ns_root}:Objects')
        server_lst.append(f'{ns_root}:Server')
        server_lst.append(f'{ns_root}:ServerStatus')
        server_lst.append(f'{ns_root}:CurrentTime')
        server_state_node = await root.get_child(server_lst)
        server_state_val = await server_state_node.get_value()
        logging.info(f'current time on Server: {server_state_val}')

        '''
        #subscribe to Alarm
        '''
        handler = SubHandler()
        sub = await client.create_subscription(1000,handler)
        handle_alm = await sub.subscribe_data_change(alarm_node)
        handle_buff = await sub.subscribe_data_change(buffer_node)
        handler.dict_reaction[alarm_node] = 'shutdown'
        handler.dict_reaction[buffer_node] = 'info'
        handler.dict_states = buffer_dict
        #ret = await sub.subscribe_events()

        '''
        #MAIN LOOP
        '''
        while not handler.shutdown:
            try:
                await asyncio.sleep(3)
                logging.debug(f'Server time: {await server_state_node.get_value()}')

            except KeyboardInterrupt:
                sub.delete()
                sys.exit(100)
                reconnect = False
            except:
                logging.error(f'{sys.exc_info()[1]}')
                logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')
                sys.exit(120)




if __name__ == "__main__":
    global reconnect
    reconnect = True
    init()

    while reconnect:
        try:
            asyncio.run(main())
        except:
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')
            logging.info(f'An error occured with the OPC Server. Trying to reconnect: {reconnect}')
            time.sleep(5)