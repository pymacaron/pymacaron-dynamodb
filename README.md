# klue-microservice-dynamodb

Serialize and deserialize Bravado objects to and from a Dynamodb table.

Within the
[klue-microservice](https://github.com/erwan-lemonnier/klue-microservice)
framework, Bravado objects are representations of JSON structures returned by a
REST api and described in a Swagger/OpenAPI specification.

'klue-microservice-dynamodb' serializes those JSON objects to DynamoDB and back
while also validating types and converting to and from dict formats to the
application specific formats defined in the Swagger spec.

'klue-microservice-dynamodb' also allows for transactional writes to DynamoDB
by acquiring/releasing locks on DynamoDB objects.

## Usage

In the Swagger file named 'myserver.yaml':

```json

  Profile:
    type: object
    description: A profile that can persist to DynamoDB
    x-persist: myserver.db.PersistentProfile
    properties:
      profile_id:
        type: string
        format: profile_id
        description: Unique Profile ID.

```

And in 'myserver.profile'

```python

from klue_dynamodb import PersistentSwaggerObject, DynamoDBItemNotFound

class PersistentProfile(PersistentSwaggerObject):
    api_name = 'myserver'
    model_name = 'Profile'
    table_name = 'some-dynamodb-table-name'
    primary_key = 'profile_id'

# And use PersistentProfile as in the examples below:

def profile_exists(profile_id):
    """Return true if this profile exists"""
    return PersistentProfile.get_table().has_item(profile_id=profile_id)

def get_profile(profile_id):
    """Retrieve profile from dynamodb or an exception if profile does not exist"""
    try:
        p = PersistentProfile.load_from_db(profile_id)
        return p
    except DynamoDBItemNotFound as e:
        raise MyOwnException("Profile %s does not exist" % profile_id)

```