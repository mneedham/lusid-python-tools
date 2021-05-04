from pathlib import Path

# tag::imports[]
import lusid
# end::imports[]

try:
    # tag::api-factory-env-variables[]
    api_factory = lusid.utilities.ApiClientFactory()
    # end::api-factory-env-variables[]
except ValueError as ex:
    pass

# tag::secrets-file[]
secrets_file = "/path/to/secrets.json"
# end::secrets-file[]
secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")

# tag::api-factory[]
api_factory = lusid.utilities.ApiClientFactory(api_secrets_filename=secrets_file)
# end::api-factory[]
