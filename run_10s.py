import json, time
import scalp_10s


def ws_loop():
    import websocket
    while True:
        try:
            symbols=[x["symbol"] for x in scalp_10s.ranked]
            if not symbols:
                time.sleep(2)
                continue
            streams=[]
            for s in symbols:
                streams.append(f"{s}@bookTicker")
                streams.append(f"{s}@aggTrade")
            url=scalp_10s.WS_BASE+"?streams="+"/".join(streams)
            ws=websocket.WebSocketApp(url,on_message=scalp_10s.on_message)
            ws.run_forever(ping_interval=15,ping_timeout=8)
        except Exception as e:
            scalp_10s.log.warning("WS reconnect: %s",e)
        time.sleep(1)


scalp_10s.ws_loop=ws_loop
main=scalp_10s.main

if __name__=="__main__":
    main()
