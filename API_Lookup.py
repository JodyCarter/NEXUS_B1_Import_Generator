from pynexusic.NEXUSIC_RESTAPI import NEXUSIC_REST
from time import time
import json
import os

def get_apik(ic_web):
    folder = 'C:\\Py\\Py_Key'
    full_path = os.path.join(folder, 'apik.json')
    apik = open(full_path)
    apik_dict = json.load(apik)
    return apik_dict[ic_web]


if __name__ == "__main__":
    ic_web = 'https://bhp.nexusic.com/test'  # US Inspection Hardware Database
    k = get_apik(ic_web)

    nexus_ic = NEXUSIC_REST(ic_web, authentication_type='APIKEY', api_key=k)

    start_time = time()
    asset_location = 'BHP / TT / TT_Aripo-WPP / Equipment / Pressure Vessels / ABH-0188'
    l2 = nexus_ic.execFunction('C2', {'Asset': asset_location})
    end_time = time()
    print(end_time-start_time)
