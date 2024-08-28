from Cryptodome.Cipher import AES
import traceback
from connections.SM_connection import get_secret

def get_decrypted_keys(sm_config,decrypt_secret,cipher_information=None):
        #Decryption of encrypted values using key, nonce and key parameters
        #Decrypted information in 'data' dictionary using dictionary comprehension
        try:
                if sm_config:
                        cipher_info=get_secret(cipher_information)
                        decryption_key=cipher_info['cipher_key']
                        decryption_nonce=cipher_info['cipher_nonce']
                        decrypt_info=get_secret(decrypt_secret)
                        data={key:AES.new(decryption_key,AES.MODE_EAX,decryption_nonce).decrypt(value).decode() for key,value in decrypt_info.items()}
                else:
                        data={key:AES.new(decrypt_secret[key]['key'],AES.MODE_EAX,decrypt_secret[key]['nonce']).decrypt_and_verify(decrypt_secret[key]['key_parameters'][0],decrypt_secret[key]['key_parameters'][1]).decode() for key,value in decrypt_secret.items()}
                return data
        except LookupError as ve:
                print(f'Key value error occured:{ve}')
        except Exception as e:
                traceback.print_exc()
