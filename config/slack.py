import requests


class MyMsg():
    def send_msg(self, msg=""):
        response = requests.post(
            'https://slack.com/api/chat.postMessage',
            headers={
                'Authorization': 'Bearer ' + 'xoxb-3130958372483-3133351108292-Os9WzPgvFiuXuQI6pUR6UM6k'
            },
            data={
                'channel': 'stock-trading',
                'text': msg
            }
        )
        print("slack message request result ==> %s" % response)
