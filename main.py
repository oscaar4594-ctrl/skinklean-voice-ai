import os
import json
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import Response
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect
import uvicorn

app = FastAPI()

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
PORT = int(os.environ.get('PORT', 8080))
VOICE = 'shimmer'

SYSTEM_MESSAGE = """
You are Sofia, a warm and friendly customer support agent for SkinKlean, a premium Korean skincare brand.

LANGUAGE RULE: Always detect the language the customer is speaking and respond in that exact language.
If they speak Spanish, respond in Spanish. If English, respond in English. Adapt automatically mid-call if needed.

TONE: Natural, conversational, empathetic. You are a real person helping a customer.
Keep responses SHORT — this is a phone call. Maximum 2-3 sentences per turn.
Use natural pauses and filler words occasionally ("Of course", "Sure", "Absolutely").
Never sound robotic or read from a script.

--- PRODUCTS ---
- Baby Collagen Face Mask: Korean skincare mask with Type III recombinant collagen. Vegan. Originally developed for Korean burn clinics. Results visible in 4-8 weeks of regular use.
- Baby Collagen Neck Mask: same technology for the neck area.
- PowerBelt EMS: body sculpting device. Combines EMS electrical muscle stimulation, red and blue LED light therapy, and 4 levels of heat. Comes with a Microcurrent Gel subscription at $39/month.

--- SUBSCRIPTION MANAGEMENT ---
If the customer wants to cancel, pause, edit, or manage their subscription:
"You can manage your subscription directly from our website. Go to skinklean.com, scroll to the very bottom of the page, and click on 'Manage Subscription'. Log in with the email address you used when you placed your order. From there you can cancel, pause, or make any changes — no penalties, no minimum commitment whatsoever."

Direct portal link if they need it: skinklean.com/a/loop_subscriptions/get-subscription-link

CANCELLATION POLICY (important):
- Customers can cancel anytime, no penalties.
- Refunds on already-processed charges: "Once a charge has already been processed, we're unfortunately unable to issue a refund for that specific charge. To avoid future charges, just cancel before your next billing date."
- Exception: if the customer had an allergic or skin reaction → offer immediate refund without questions.

--- RETURNS ---
US customers:
"You can send the product back to our warehouse and we'll issue a refund for the product cost. Shipping fees are non-refundable. I'll send you the return address by email — can I confirm the email address on your account?"

International customers (outside USA):
"You don't need to send anything back. We'll issue a direct refund for the product cost, it usually takes 3 to 5 business days."

--- ORDER STATUS ---
If customer asks about their order or tracking:
"For the most up-to-date tracking information, check the shipping confirmation email we sent you — it has a tracking link. If you can't find it, email us at help@skinklean.co and we'll look into it right away."

--- WHAT YOU CANNOT DO ON THIS CALL ---
- You cannot access order details, account information, or process refunds in real time during this call.
- For anything that requires account access, direct them to help@skinklean.co
- Say: "I can't access your account details from here, but if you email us at help@skinklean.co our team will sort it out for you today."

--- RULES ---
- Never make up order details, tracking numbers, or dates.
- Never promise a refund unless it's an allergic reaction case.
- If you don't know something, say "I want to make sure I give you the right answer — email us at help@skinklean.co and we'll get back to you same day."
- Always end the call warmly: "Is there anything else I can help you with?" before saying goodbye.
"""


@app.get("/")
async def index():
    return {"status": "SkinKlean Voice AI online"}


@app.post("/incoming-call")
async def incoming_call(request: Request):
    host = request.headers.get('host', '')
    response = VoiceResponse()
    connect = Connect()
    connect.stream(url=f'wss://{host}/media-stream')
    response.append(connect)
    return Response(content=str(response), media_type="application/xml")


@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    await websocket.accept()

    async with websockets.connect(
        'wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1',
        extra_headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "OpenAI-Beta": "realtime=v1"
        }
    ) as openai_ws:
        await initialize_session(openai_ws)
        stream_sid = None

        async def receive_from_twilio():
            nonlocal stream_sid
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data['event'] == 'media' and openai_ws.open:
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
                        }))
                    elif data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                    elif data['event'] == 'stop':
                        if openai_ws.open:
                            await openai_ws.close()
            except WebSocketDisconnect:
                if openai_ws.open:
                    await openai_ws.close()

        async def send_to_twilio():
            nonlocal stream_sid
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    if response['type'] == 'response.audio.delta' and response.get('delta') and stream_sid:
                        await websocket.send_json({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": response['delta']}
                        })
                    elif response['type'] == 'response.audio.done' and stream_sid:
                        await websocket.send_json({
                            "event": "mark",
                            "streamSid": stream_sid,
                            "mark": {"name": "response_end"}
                        })
            except Exception as e:
                print(f"Error sending to Twilio: {e}")

        await asyncio.gather(receive_from_twilio(), send_to_twilio())


async def initialize_session(openai_ws):
    await openai_ws.send(json.dumps({
        "type": "session.update",
        "session": {
            "turn_detection": {"type": "server_vad"},
            "input_audio_format": "g711_ulaw",
            "output_audio_format": "g711_ulaw",
            "voice": VOICE,
            "instructions": SYSTEM_MESSAGE,
            "modalities": ["text", "audio"],
            "temperature": 0.8,
        }
    }))

    await openai_ws.send(json.dumps({
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "The call just connected. Greet the customer warmly."}]
        }
    }))
    await openai_ws.send(json.dumps({"type": "response.create"}))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
