

TEST_PARAMS = {
    "contract_address": "KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6"
}

def runTask(params={}):
    PLACEHOLDER = "NO_ADDRESS_SPECIFIED"
    print(params)
    contract_address = params.get("contract_address")
    print("Running tezos contracts statistics task for address: ", contract_address)

    if contract_address == PLACEHOLDER:
        print("No address specified, exiting")
        raise Exception("No address specified, exiting")

