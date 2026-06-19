import cv2
import time
from websocket import create_connection

CAMERA_ID = "jetson_node_01"
# Switch protocol to wss:// for Render cloud environments
WS_URL = f"wss://jelaria-stream.onrender.com/ws/upload/{CAMERA_ID}"

cap = cv2.VideoCapture("sample.mp4")
fps = 15.0
frame_delay = 1.0 / fps

print("Connecting to live WebSocket stream...")
try:
    ws = create_connection(WS_URL)

    while cap.isOpened():
        start_time = time.time()

        ret, frame = cap.read()
        if not ret:
            print("Looping video file...")
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            continue

        frame = cv2.resize(frame, (640, 360))
        encode_param = [
        cv2.IMWRITE_JPEG_QUALITY, 35,
        cv2.IMWRITE_JPEG_OPTIMIZE, 1
    ]
        success, encoded_image = cv2.imencode('.jpg', frame, encode_param)
        # Send raw frame binary over the single, permanent socket
        ws.send_binary(encoded_image.tobytes())

        # Frame rate sleep management
        elapsed = time.time() - start_time
        time_to_wait = frame_delay - elapsed
        if time_to_wait > 0:
            time.sleep(time_to_wait)

except Exception as e:
    print(f"Streaming error: {e}")
finally:
    cap.release()
    try: ws.close()
    except: pass
