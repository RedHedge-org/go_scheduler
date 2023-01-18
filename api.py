import sanic
import time

app = sanic.Sanic(name="test")


last_calls = {
    "test_1": time.time(),
    "test_2": time.time(),
    "test_3": time.time(),
}

# simple endpoint
@app.route("/test_1")
def test(request):
    time_diff = time.time() - last_calls["test_1"]
    last_calls["test_1"] = time.time()
    print("test_1", time_diff)

    return sanic.response.json({"test": "ok"})


@app.route("/test_2")
def test(request):
    time_diff = time.time() - last_calls["test_2"]
    print("test_2", time_diff)
    last_calls["test_2"] = time.time()
    return sanic.response.json({"test": "ok"})


@app.route("/test_3")
def test(request):
    time_diff = time.time() - last_calls["test_3"]
    print("test_3", time_diff)
    last_calls["test_3"] = time.time()
    return sanic.response.json({"test": "ok"})


if __name__ == "__main__":
    app.run(host="localhost", port=8000)
