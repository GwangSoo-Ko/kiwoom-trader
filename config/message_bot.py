import requests
import telegram


class MyMsg():
    def send_msg_slack(self, msg=""):
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

    def send_msg_telegram(self, msg=""):
        token = "5150290010:AAHJ6Dg7r_E_qf76xXC-DjOCBJbcn925C9k"
        bot = telegram.Bot(token)
        test_chat_id = 230299001
        chat_id = -642881208
        bot.sendMessage(chat_id=chat_id, text=msg)
