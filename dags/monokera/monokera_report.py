from datetime import date
import pysftp
import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine


class MonokeraReport():
    def __init__(self):
        self.POSTGRESQL_HOST = None
        self.POSTGRESQL_PORT = None
        self.POSTGRESQL_USER = None
        self.POSTGRESQL_PASSWORD = None
        self.POSTGRESQL_DATABASE = None
        self.POSTGRESQL_SCHEMA = None
        self.SFTP_HOST = None
        self.SFTP_USER = None
        self.SFTP_PASSWORD = None
        self.SFTP_LOCAL_FILE = None
        self.SFTP_FILENAME = None

    def create_engine(self, host=None, port=None, user=None, password=None, database=None, schema=None):
        try:
            engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
            return engine
        except Exception as e:
            raise e

    def execute_query(self, conn, query):
        cursor: pg2.Cursor = conn.cursor()
        cursor.execute(query)

        # result: pandas.DataFrame = cursor.fetch_dataframe()
        result = pd.DataFrame(cursor)
        if result is None:
            return None
        elif result.empty:
            return None
        else:
            return result

    def read_file_local(self, url):
        print(f'incia el proceso: ----------read_file------local')
        try:
            file = pd.read_csv(url,
                               sep=',',
                               header=0,
                               encoding='UTF-8'
                               )
            return file
        except Exception as e:
            print(f'fallo la conexion')
            raise

    def read_file(self, file):
        print(f'incia el proceso: ----------read_file------{file}')
        try:
            sftp = self.sftp_connector(self.SFTP_HOST, self.SFTP_USER, self.SFTP_PASSWORD,
                                      2222, self.SFTP_LOCAL_FILE)
            local_path = f"{self.SFTP_FILENAME}"
            remote_path = f"{self.SFTP_LOCAL_FILE}/{self.SFTP_FILENAME}"

            with sftp.open(remote_path) as f:
                f.prefetch()
                new_file = pd.read_csv(f, sep=',',
                                       header=0,
                                       encoding='latin-1'
                                       )
                f.close()
            return new_file
        except Exception as e:
            print(f'fallo la conexion')
            raise

    def get_conn(self, engine):
        print('incia el proceso: ----------get_conn------')

        conn = engine.connect()
        # conn.execute("Use secondary roles all")
        # conn.execute("Use role latam_sandbox_admin")
        return conn

    def load(self, engine, businessObject: dict, schema):
        try:
            print('load')
            connection = self.get_conn(engine)
            for name, value in businessObject.items():
                table_name = name
                value.to_sql(table_name, connection, schema=schema, method='multi', if_exists='replace',
                             index=False)
            connection.commit()
            connection.close()
        except Exception as e:
            raise

    def separateBusinessObjects(self, data):
        try:
            print('incia el proceso: ----------separateBusinessObjects------')

            Policy = data[
                ['id', 'policy_number', 'policy_start_date', 'policy_end_date', 'policy_type', 'insurance_company']]
            Insured = data[
                ['first_name', 'last_name', 'email', 'insured_name', 'insured_gender', 'gender', 'insured_age',
                 'insured_address', 'insured_city', 'insured_state', 'insured_postal_code',
                 'insured_country', ]]
            Premium = data[['policy_number', 'premium_amount', 'deductible_amount', 'coverage_limit', ]]
            Payments = data[['policy_number', 'payment_status', 'payment_date', 'payment_amount', 'payment_method', ]]
            Claims = data[['policy_number', 'claim_status', 'claim_date', 'claim_amount', 'claim_description']]
            Agents = data[['agent_name', 'agent_email', 'agent_phone', ]]

            return {'Policy': Policy, 'Insured': Insured, 'Premium': Premium, 'Payments': Payments, 'Claims': Claims,
                    'Agents': Agents}
        except Exception as e:
            print(f'fallo proceso de separacion')

    def preparePolicy(self, businessObjects):
        try:
            print('incia el proceso: ----------preparePolicy------')
            businessObjects['fecha_cargue'] = date.today()
            return businessObjects
        except Exception as e:
            print(f'fallo metodo preparePolicy')
            raise

    def prepareInsured(self, businessObjects):
        try:
            print('incia el proceso: ----------prepareInsured------')
            businessObjects['insured_gender'] = businessObjects['insured_gender'].combine_first(
                businessObjects['gender'])
            businessObjects = businessObjects[['first_name', 'last_name', 'email', 'insured_name', 'insured_gender',
                                               'insured_age', 'insured_address', 'insured_city',
                                               'insured_state', 'insured_postal_code', 'insured_country', ]]
            businessObjects['fecha_cargue'] = date.today()

            return businessObjects
        except Exception as e:
            print(f'fallo metodo prepareInsured')
            raise

    def preparePremium(self, businessObjects):
        try:
            print('incia el proceso: ----------preparePremium------')
            businessObjects['fecha_cargue'] = date.today()
            return businessObjects
        except Exception as e:
            print(f'fallo metodo preparePolicy')
            raise

    def preparePayments(self, businessObjects):
        try:
            print('incia el proceso: ----------preparePayments------')
            businessObjects['fecha_cargue'] = date.today()
            return businessObjects
        except Exception as e:
            print(f'fallo metodo preparePolicy')
            raise

    def prepareClaims(self, businessObjects):
        try:
            print('incia el proceso: ----------prepareClaims------')
            businessObjects['fecha_cargue'] = date.today()
            return businessObjects
        except Exception as e:
            print(f'fallo metodo preparePolicy')
            raise

    def prepareAgents(self, businessObjects):
        try:
            print('incia el proceso: ----------prepareAgents------')
            businessObjects['fecha_cargue'] = date.today()
            return businessObjects
        except Exception as e:
            print(f'fallo metodo preparePolicy')
            raise

    def preparebusinessObjects(self, business_name: str, businessObject: pd.DataFrame):
        try:
            print('incia el proceso: ----------prepareBusinessObjects------')
            if business_name == 'Policy' and not businessObject.empty:
                print('Policy')
                return {'Policy': self.preparePolicy(businessObject)}
            elif business_name == 'Insured' and not businessObject.empty:
                print('Insured')
                return {'Insured': self.prepareInsured(businessObject)}
            elif business_name == 'Premium' and not businessObject.empty:
                print('Premium')
                return {'Premium': self.preparePremium(businessObject)}
            elif business_name == 'Payments' and not businessObject.empty:
                print('Payments')
                return {'Payments': self.preparePayments(businessObject)}
            elif business_name == 'Claims' and not businessObject.empty:
                print('Claims')
                return {'Claims': self.prepareClaims(businessObject)}
            elif business_name == 'Agents' and not businessObject.empty:
                print('Agents')
                return {'Agents': self.prepareAgents(businessObject)}
            else:
                print('No hay un objeto de negocio valido')


        except Exception as e:
            raise

    def get_variables(self, **kwargs):
        try:
            self.POSTGRESQL_HOST = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_HOST')
            self.POSTGRESQL_PORT = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_PORT')
            self.POSTGRESQL_USER = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_USER')
            self.POSTGRESQL_PASSWORD = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_PASSWORD')
            self.POSTGRESQL_DATABASE = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_DB')
            self.POSTGRESQL_SCHEMA = kwargs.get('POSTGRESQL_CONN').get('POSTGRESQL_SCHEMA')

            self.SFTP_HOST = kwargs.get('SFTP_CONN').get('SFTP_HOST')
            self.SFTP_USER = kwargs.get('SFTP_CONN').get('SFTP_USER')
            self.SFTP_PASSWORD = kwargs.get('SFTP_CONN').get('SFTP_PASSWORD')
            self.SFTP_LOCAL_FILE = kwargs.get('SFTP_CONN').get('LOCAL_FILE')
            self.SFTP_FILENAME = kwargs.get('SFTP_CONN').get('FILENAME')

        except Exception as e:
            raise e

    def sftp_connector(self, host, username, password, port, path):
        try:
            print('incia el proceso: ----------sftp_connector------')
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            srv = pysftp.Connection(host='localhost', username=username, password=password, port=port, cnopts=cnopts)
            return srv
        except Exception as e:
            raise e

    def process(self, business_Objects, postgresql_engine):
        try:

            for business_name, business_Object in business_Objects.items():
                load_businessObject = self.preparebusinessObjects(business_name, business_Object)
                self.load(postgresql_engine, load_businessObject, self.POSTGRESQL_SCHEMA)
        except Exception:
            raise

    def run(self, **kwargs):
        try:
            print('incia el proceso: ----------run-----')
            self.get_variables(**kwargs)
            postgresql_engine = self.create_engine(self.POSTGRESQL_HOST,
                                                   self.POSTGRESQL_PORT,
                                                   self.POSTGRESQL_USER,
                                                   self.POSTGRESQL_PASSWORD,
                                                   self.POSTGRESQL_DATABASE,
                                                   self.POSTGRESQL_SCHEMA)

            data = self.read_file(self.SFTP_FILENAME)
            business_Objects = self.separateBusinessObjects(data)
            self.process(business_Objects, postgresql_engine)

        except Exception as e:
            raise e

#
#POSTGRESQL_CONN = {}
#POSTGRESQL_CONN['POSTGRESQL_HOST'] = '0.0.0.0'  #os.getenv("POSTGRES_HOST")
#POSTGRESQL_CONN['POSTGRESQL_USER'] = 'admin'      #os.getenv("POSTGRES_USER")
#POSTGRESQL_CONN['POSTGRESQL_PASSWORD'] = 'root'   #os.getenv("POSTGRES_PASSWORD")
#POSTGRESQL_CONN['POSTGRESQL_DB'] = 'monokera'     #os.getenv("POSTGRES_DB")
#POSTGRESQL_CONN['POSTGRESQL_PORT'] = 5433         #os.getenv("POSTGRES_PORT")
#POSTGRESQL_CONN['POSTGRESQL_SCHEMA'] = 'monokera' #os.getenv("POSTGRES_SCHEMA")
#
#SFTP_CONN = {}
#SFTP_CONN['SFTP_HOST'] = '0.0.0.0'
#SFTP_CONN['SFTP_USER'] = 'foo'
#SFTP_CONN['SFTP_PASSWORD'] = 'pass'
#SFTP_CONN['LOCAL_FILE'] = 'upload/'
#SFTP_CONN['FILENAME'] = 'MOCK_DATA.csv'
#
#kwargs = {'POSTGRESQL_CONN':POSTGRESQL_CONN,'SFTP_CONN':SFTP_CONN}
#MonokeraReport().run(**kwargs)
#