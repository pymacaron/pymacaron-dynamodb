import logging
import boto3
import json
import pprint
import types
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
# Recursively map a dynamodb dict into a swagger dict
#

def _normalize_item(spec, v):
    if spec['type'].lower() == 'boolean':
        v = True if v else False
    elif spec['type'].lower() == 'number':
        v = float(v)
    elif spec['type'].lower() == 'integer':
        v = int(v)
    return v


def _normalize_object(api, definitions, ref, v):
    assert isinstance(v, dict), "should be a dictionary: %s" % v
    model_name = ref.split('/')[-1]
    # log.debug("Normalizing dict of type %s" % model_name)
    v = _normalize_dict(api, definitions, definitions[model_name]['properties'], v)
    return v


def _normalize_list(api, definitions, items, l):
    # items looks like:
    # {   '$ref': '#/definitions/UserPicture',
    #     'x-scope': [...]
    # }
    # or:
    #

    l = list(l)
    # log.debug("Normalizing list: " + pprint.pformat(l))
    ll = []

    for v in l:
        if '$ref' in items:
            # log.debug("Normalizing array item: %s" % pprint.pformat(v, indent=4))
            v = _normalize_object(api, definitions, items['$ref'], v)
        elif 'type' in items and items['type'] in ('string', 'boolean', 'integer'):
            v = _normalize_item(items, v)
        else:
            log.debug("items is %s" % items)
            assert 0, "Not implemented"
#         elif k_spec['type'].lower() == 'array':
#             assert isinstance(v, list), "should be a list: %s" % v
#             log.debug("Normalizing array: %s" % pprint.pformat(v, indent=4))
#             log.debug("Normalizing array has spec: %s" % pprint.pformat(k_spec, indent=4))
#             v = _normalize_list(api, definitions, k_spec['items'], v)
#         else:
#             v = _normalize_item(v)

        ll.append(v)
    return ll


def _normalize_dict(api, definitions, model_properties, d):
    d = dict(d)

    # Go through all keys in dict and normalize DynamoDB types to json
    for k, v in list(d.items()):

        k_spec = model_properties[k]

        if '$ref' in k_spec:
            v = _normalize_object(api, definitions, k_spec['$ref'], v)
        elif k_spec['type'].lower() == 'array':
            assert isinstance(v, list), "should be a list: %s" % v
            v = _normalize_list(api, definitions, k_spec['items'], v)
        else:
            v = _normalize_item(k_spec, v)

        d[k] = v
    return d


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


    @classmethod
    def import_childclass(self, object):
        components = object.__persistence_class__.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod


    def save_to_db(self):
        global model_to_persistent_class

        # Do we need to run setup on the persistence class?
        if self.__class__.__name__ not in model_to_persistent_class:
            c = PersistentSwaggerObject.import_childclass(self)
            c.setup(c)

        childclass = model_to_persistent_class[self.__class__.__name__]

        j = childclass.api.model_to_json(self)
        log.debug("Storing json into DynamoDB/%s: %s" % (childclass.table_name, json.dumps(j, indent=2)))

        childclass.table.put_item(Item=j)


    @classmethod
    def to_model(childclass, item):
        PersistentSwaggerObject.setup(childclass)

        if '__persistence_class__' in item:
            del item['__persistence_class__']

        # Normalize DynamoDB dict into Swagger json dict
        spec = childclass.api.api_spec.swagger_dict
        item = _normalize_dict(
            childclass.api,
            spec['definitions'],
            spec['definitions'][childclass.model_name]['properties'],
            item
        )

        # Convert from python dict to Swagger object
        item = childclass.api.json_to_model(childclass.model_name, item)

        # Monkey-patch this model so we can store it later
        item.save_to_db = types.MethodType(childclass.save_to_db, item)

        # log.info("Loaded %s from table %s: %s" % (childclass.model_name, childclass.table_name, pprint.pformat(item, indent=4)))
        return item
