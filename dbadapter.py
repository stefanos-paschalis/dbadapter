import time
import datetime
import functools
import cx_Oracle
from influxdb import InfluxDBClient


###########################################################################
# Database credentials
###########################################################################
Credentials = {
        'username1@database1': { 'password': 'xxxxxxxxx' },
        'username2@database2': { 'password': 'yyyyyyyyy' }
    }


###########################################################################
# Metrics
###########################################################################
Metrics = {
        1: {
            'measurement': 'CCHOURDATA80',
            'tags':'80% Cost Control Data in last hour',
            'login': 'sysadm@bscsrep',
            'influxdb': 'dbmon',
            'interval': 10,
            'unit': 'seconds',
            'query': """select count(1) 
                        from 
                        ( 
                            select entry_date, session_id, msisdn 
                            from bilsys.cc_dbg_ocs_bea_params c 
                            where entry_date >= (sysdate-1/24)
                            and business_case like '%_80'
                            group by entry_date, session_id, msisdn 
                        )"""
        },

        2: {
            'measurement': 'CCHOURDATA100',
            'tags':'100% Cost Control Data in last hour',
            'login': 'sysadm@bscsrep',
            'influxdb': 'dbmon',
            'interval': 20,
            'unit': 'seconds',
            'query': """select count(1) 
                        from 
                        ( 
                            select entry_date, session_id, msisdn 
                            from bilsys.cc_dbg_ocs_bea_params c 
                            where entry_date >= (sysdate-1/24)
                            and business_case like '%_100'
                            group by entry_date, session_id, msisdn 
                        )"""
        }

    }


###########################################################################
# Exceptions
###########################################################################
class CustomException(Exception):
    """Custom exception"""
    pass
   
    
###########################################################################
# Global functions
###########################################################################
def logo():
    print("--------------------------------------------------------------------")
    print(" Designed & Developed on Dec 2019 by ")
    print(" Billing Systems Section for Mobile ")
    print(" IT BSS & OSS for Fixed & Mobile Division ")
    print(" COSMOTE S.A. ")
    print(" 99 Kifissias Ave., 151 24 Maroussi, Athens, Greece ")
    print(" Tel.: 00306974391906 ")
    print(" email: BillingSystemsSection@cosmote.gr ")
    print("--------------------------------------------------------------------")


def str_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


###########################################################################
# Job
###########################################################################
class Job(object):
    """This class controls scheduling of metric jobs"""
    def __init__(self, key, interval, unit):
        self.key = key
        self.interval = interval
        self.unit = unit
        self.last_run = None
        self.next_run = None

    def should_run(self):
        return (self.last_run == None or datetime.datetime.now() >= self.next_run)

    def delta(self):
        if self.unit == 'seconds':
            return datetime.timedelta(seconds=self.interval)
        elif self.unit == 'minutes':
            return datetime.timedelta(seconds=(self.interval*60))
        elif self.unit == 'hours':
            return datetime.timedelta(seconds=(self.interval*60*60))
        elif self.unit == 'days':
            return datetime.timedelta(seconds=(self.interval*60*60*24))
        elif self.unit == 'weeks':
            return datetime.timedelta(seconds=(self.interval*60*60*24*7))

    def executed(self):
        self.last_run = datetime.datetime.now()
        self.next_run = self.last_run + self.delta()


###########################################################################
# Oracle database
###########################################################################
class Oracle:
    """This class controls the oracle database connection & operations"""
    _login = ''
    _username = ''
    _password = ''
    _database = ''
    _connection = None
    _instances = []
    _status = {}

    def __init__(self, login):
        for key in Credentials:
            if key == login:
                self._login = login
                self._username = self._login.split('@')[0]
                self._database = self._login.split('@')[1]
                self._password = Credentials[key]['password']

    @staticmethod
    def set_status(login, status):
        Oracle._status[login] = status

    @staticmethod
    def get_status(login):
        return Oracle._status[login]

    @staticmethod
    def exists(login):
        return login in Oracle._status.keys()

    @staticmethod
    def set_instance(connection):
        Oracle._instances.append(connection)

    @staticmethod
    def get_instance():
        return self._connection

    @staticmethod
    def disconect_all():
        for instance in Oracle._instances:
            #instance.cursor.close()
            instance.close()
        
    def connect(self):
        print('\n[{0}] Oracle.connect: connecting to database: {1}'.format(str_time(), self._login))
        if self._connection == None or not Oracle.exists(self._login):
            try:
                self._connection = cx_Oracle.connect(self._username+'/'+self._password+'@'+self._database)
                print('[{0}] Oracle.connect: successfully connected to database: {1}'.format(str_time(), self._database));
                Oracle.set_status(self._login, True)
                Oracle.set_instance(self._connection)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                if error.code == 1017:
                    print('Oracle.connect: please check your credentials')
                else:
                    print('Oracle.connect: database connection error')
                Oracle.set_status(self._login, False)
                self._connection = None
                raise
        else:
            self._connection = None
            raise Exception('Oracle.connect: status in invalid')

    def disconnect(self):
        if self._connection and Oracle._status[self._login]:
            self._connection.cursor.close()
            self._connection.close()

    def execute(self, query):
        cursor = None
        if self._connection and Oracle._status[self._login]:
            cursor = self._connection.cursor()
            cursor.arraysize = 1
            try:
                print('\n[{0}] Oracle.execute: database={1}'.format(str_time(), self._database))
                print('[{0}] Oracle.execute: query={1}'.format(str_time(), query))
                cursor.execute(query)
                #for row in cursor.fetchall():
                #    print('Oracle.execute: returned rows')
                #    print(row)
                #cursor.close()
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print(error.code)
                print(error.message)
                print(error.context)
                raise
        else:
            raise Exception('Oracle.execute: unable to execute: %s' % query)
        return cursor
    

###########################################################################
# Influxdb database
###########################################################################
class Influx:
    """This class controls the influx database connections & operations"""
    host = None
    port = None
    database = None
    client = None

    def __init__(self, host='10.95.14.58', port=8086, database='dbmon'):
    #def __init__(self, host='localhost', port=8086, database='dbmon'):
        self.host = host
        self.port = port
        self.database = database
        
    def connect(self):
        try:
            print('\n[{0}] Influx.connect: connecting to host: {1}, port:{2}, database:{3}'.format(str_time(), self.host, self.port, self.database))
            self.client = InfluxDBClient(self.host, self.port)
            self.client.switch_database(self.database)
        except Exception as e:
                error, = e.args
                print(error.code)
                print(error.message)
                print(error.context)
        finally:
            print('[{0}] Influx.connect: successfully connected to database: {1}'.format(str_time(), self.database));

    def insert(self, metric, cursor):
        data = []
        for row in cursor.fetchall():
            try:
                json_body = {
                    'measurement': metric['measurement'],
                    'tags': {
                        'database': metric['login'].split('@')[1],
                        'description': metric['tags']
                        },
                    'time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'fields': {
                        'value': row[0]
                        }
                    }
                data.append(json_body)
                print("\n[{0}] Influx.insert: inserting data: {1}".format(str_time(), data))
                self.client.write_points(data)
            except Exception as e:
                error, = e.args
                print(error.code)
                print(error.message)
                print(error.context)
            finally:
               print("[{0}] Influx.insert: success".format(str_time()))        


###########################################################################
# Main
###########################################################################
def main():
    jobs = []

    logo()
    print('\n[{0}] Module dbadapter has been started'.format(datetime.datetime.now()))

    dbmon = Influx()
    dbmon.connect()

    while True:
        for key, metric in Metrics.items():
            login = metric['login']

            job = None
            for j in jobs:
                if j.key == key:
                    job = j
                    break
                    
            if job == None:
                job = Job(key, metric['interval'], metric['unit'])
                jobs.append(job)
                
            if job.should_run():
                if not Oracle.exists(login):
                    bscs = Oracle(login)
                    bscs.connect()
                if Oracle.get_status(login) == True:
                    cursor = bscs.execute(metric['query'])
                    if dbmon:
                        dbmon.insert(metric, cursor)
                job.executed()

        time.sleep(1)
            

###########################################################################
# Module
###########################################################################
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        Oracle.disconect_all()
    finally:
        print('\n[{0}] Module dbadapter has been stopped'.format(str_time()))

    

