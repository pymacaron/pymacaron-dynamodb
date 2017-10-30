import logging
import boto3
import json
from klue.swagger.apipool import ApiPool
from klue_microservice.config import get_config
from klue_microservice.exceptions import KlueMicroServiceException


log = logging.getLogger(__name__)


db = None


def get_dynamodb():
    global db
    if not db:
        conf = get_config()
        log.info("Dynamodb setup against region:%s access_key_id:%s, aws_secret:%s***" % (conf.aws_default_region, conf.aws_access_key_id, conf.aws_secret_access_key[0:8]))
        db = boto3.resource(
            'dynamodb',
            region_name=conf.aws_default_region,
            aws_access_key_id=conf.aws_access_key_id,
            aws_secret_access_key=conf.aws_secret_access_key
        )

        return db

# Exception raised if no item found
class DynamoDBItemNotFound(KlueMicroServiceException):
    pass

#
# Take a Swagger JSON dict and persist it to/from dynamodb
#


model_to_persistent_class = {}


class PersistentSwaggerObject():

    @staticmethod
    def setup(childclass):
        # A child class should override the following class attributes
        # api_name
        # model_name
        # table_name
        # primary_key
        #
        # setup() sets the child class's attributes:
        # table
        # api
        # model

        if not hasattr(childclass, 'api'):
            api_name = getattr(childclass, 'api_name')
            model_name = getattr(childclass, 'model_name')
            table_name = getattr(childclass, 'table_name')
            primary_key = getattr(childclass, 'primary_key')

            assert api_name
            assert model_name
            assert table_name
            assert primary_key

            log.info("Initializing %s with api_name=%s, model_name=%s, table_name=%s, primary_key=%s" % (
                childclass.__name__,
                api_name,
                model_name,
                table_name,
                primary_key,
            ))

            global model_to_persistent_class
            model_to_persistent_class[model_name] = childclass

            api = getattr(ApiPool, api_name)
            model = getattr(getattr(api, 'model'), model_name)
            table = get_dynamodb().Table(table_name)

            log.info("Setting %s.table = %s" % (childclass.__name__, table))

            setattr(childclass, 'api', api)
            setattr(childclass, 'model', model)
            setattr(childclass, 'table', table)


    @classmethod
    def get_table(childclass):
        PersistentSwaggerObject.setup(childclass)
        return getattr(childclass, 'table')


    @classmethod
    def load_from_db(childclass, key):
        PersistentSwaggerObject.setup(childclass)

        response = childclass.table.get_item(
            Key={
                childclass.primary_key: key,
            }
        )

        if 'Item' not in response:
            raise DynamoDBItemNotFound("Table %s has no item with %s=%s" % (childclass.table_name, childclass.primary_key, key))

        return childclass.to_model(response['Item'])


    def save_to_db(self):
        global model_to_persistent_class
        childclass = model_to_persistent_class[self.__class__.__name__]
        PersistentSwaggerObject.setup(childclass)

        j = childclass.api.model_to_json(self)
        log.debug("Storing json into DynamoDB/%s: %s" % (childclass.table_name, json.dumps(j, indent=2)))
        childclass.table.put_item(Item=j)


    @classmethod
    def to_model(childclass, item):
        PersistentSwaggerObject.setup(childclass)
        # TODO: convert from dynamodb format to python dict
        # Convert from python dict to Swagger object
        m = childclass.model(**item)
        log.info("Loaded from %s the object %s" % (childclass.table_name, m))
        return m
