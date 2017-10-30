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

