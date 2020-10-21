# coding=UTF-8

import vk
from vk_parsing.parser import send_message

# Several tokens for 1 user don't work. Need to buy more vk pages.
_user_tokens = ['put your user token here',
                'like this'
                ]


_service_tokens = [
    'put your service tokens here',
    'as much as possible']

# Split tokens by groups and pass each group to collect_user_data() thread
def groups(s_tokens_n=6, service_tokens= None, user_tokens=None):  # tokens_n is number of service tokens in each group
    """
    :param s_tokens_n: number of service tokens in each group
    :param service_tokens: a list of vk service tokens
    :param user_tokens: a list of vk user tokens
    :return: groups of service and user tokens
    """
    if service_tokens is None:
        service_tokens = _service_tokens
    if user_tokens is None:
        user_tokens = _user_tokens

    service_groups = []
    users_groups = []
    groups_n = len(service_tokens) // s_tokens_n
    u_tokens_n = len(user_tokens) // groups_n
    if u_tokens_n < 1 or groups_n < 1:
        print('Minimum {} user tokens and service tokens are required. Exiting'.format(groups_n))
        send_message('Add more user tokens. Exiting')
        exit()

    while len(service_tokens) > 0:
        for group in range(groups_n):
            if len(service_groups) < groups_n:
                service_groups.append([service_tokens.pop()])
            else:
                service_groups[group].append(service_tokens.pop())
            if len(service_tokens) == 0: break

    while len(user_tokens) > 0:
        for group in range(groups_n):
            if len(users_groups) < groups_n:
                users_groups.append([user_tokens.pop()])
            else:
                users_groups[group].append(user_tokens.pop())
            if len(user_tokens) == 0: break

    return service_groups, users_groups

class Session:

    def __init__(self, service_tokens=None, user_tokens=None):
        if service_tokens is None:
            self._service_tokens = _service_tokens
        else: self._service_tokens = service_tokens

        if user_tokens is None:
            self._user_tokens = _user_tokens
        else: self._user_tokens = user_tokens

        self._i = 0
        self._y = 0
        self._service_vk = None
        self._user_vk = None

    def update_public_key(self):

        service_session = vk.Session(access_token=self._service_tokens[self._i])
        self._service_vk = vk.API(service_session)

        if self._i is len(self._service_tokens) - 1:
            self._i = 0
        else:
            self._i += 1

        print('The service key has been changed:\n')
        return self._service_vk

    def update_user_key(self):
        user_session = vk.Session(access_token=self._user_tokens[self._y])

        self._user_vk = vk.API(user_session)

        if self._y is len(self._user_tokens) - 1:
            self._y = 0
        else:
            self._y += 1
        print('The user key has been changed\n')
        return self._user_vk

    def get_user_vk(self):
        if self._user_vk is not None:
            return self._user_vk
        else: return self.update_user_key()

    def get_service_vk(self):
        if self._service_vk is not None:
            return self._service_vk
        else: return self.update_public_key()

    def get_service_tokens_n(self):
        return len(self._service_tokens)

    def get_user_tokens_n(self):
        return len(self._user_tokens)

    def get_user_token(self):
        return self._user_tokens[self._y][:5]

    def get_service_token(self):
        return self._service_tokens[self._i][:5]
