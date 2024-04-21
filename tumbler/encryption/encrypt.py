# coding=utf-8


key_index = 3
encrypt_dict = {}
decrypt_dict = {}


def init_encrypt():
    global encrypt_dict, decrypt_dict
    for i in range(26):
        left = chr((i + key_index) % 26 + ord('a'))
        right = chr(ord('a') + i)
        encrypt_dict[left] = right
        decrypt_dict[right] = left
        left = chr((i + key_index) % 26 + ord('A'))
        right = chr(ord('A') + i)
        encrypt_dict[left] = right
        decrypt_dict[right] = left


init_encrypt()


def my_encrypt(key):
    global encrypt_dict
    secret_message = ""
    for a in key:
        if a in encrypt_dict.keys():
            secret_message += encrypt_dict[a]
        else:
            secret_message += a
    return secret_message


def my_decrypt(key):
    global decrypt_dict
    secret_message = ""
    for a in key:
        if a in decrypt_dict.keys():
            secret_message += decrypt_dict[a]
        else:
            secret_message += a
    return secret_message
