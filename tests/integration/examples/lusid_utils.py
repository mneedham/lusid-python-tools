from pathlib import Path
import lusid

# tag::secrets-file[]
secrets_file = "/path/to/secrets.json"
# end::secrets-file[]
secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")

# tag::api-factory[]
api_factory = lusid.utilities.ApiClientFactory(api_secrets_filename=secrets_file)
# end::api-factory[]
