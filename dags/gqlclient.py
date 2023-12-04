from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport


transport = AIOHTTPTransport(url="http://localhost:8080/graphql")

client = Client(transport=transport, fetch_schema_from_transport=True,parse_results=True)

createApplication = gql(
    """
    mutation {
        createApplication(request: {
            name: "dbt"
            techType: "dbt",
            techSubType: "dbt",
            applicationProperties: [
                {propertyName:"business_date",propertyValue:"30-11-2023"}
            ]
        }) {
            name
            techType
            techSubType
        }	
    }
    """
)
try:
    result = client.execute(createApplication)
    print(result)
except Exception as e:
    print(f"An error occurred: {e}")


getApplication = gql(
    """
    query{
        getApplication(applicationId:"1"){
            name
        }
    }
    """
)
try:
    result = client.execute(getApplication)
    print(result)
except Exception as e:
    print(f"An error occurred: {e}")


