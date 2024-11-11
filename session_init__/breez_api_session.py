from breeze_connect import BreezeConnect

api_key="f65&396497Vq0161W2d18ngV35%5755@"
api_secret="5l88!0708cJ04~74!X1V71a90j4~392+"
session_token="48831334"


def create_multiple_breeze_instances(no_instances) :
    global BreezeConnect
    global api_key,api_secret,session_token
    breeze_instances = []
    for intntance in range(1,no_instances+1) :
        breeze = BreezeConnect(api_key=api_key)
        breeze.generate_session(api_secret=api_secret,
                                session_token=session_token)
        
        breeze_instances.append(breeze)

    return breeze_instances    



# Initialize SDK
breeze = BreezeConnect(api_key=api_key)

import urllib
print("https://api.icicidirect.com/apiuser/login?api_key="+urllib.parse.quote_plus("your_api_key"))

# Generate Session
breeze.generate_session(api_secret=api_secret,
                        session_token=session_token)

print('BREEZE SESSION CRETAED')   



        

    





