const http = require("http");

module.exports = {
  notify_n8n: function (args, callback) {
    const deviceId = args[0];

    const payload = JSON.stringify({
      deviceId: deviceId,
      event: "inform",
      informEvent: "2 PERIODIC",
      timestamp: new Date().toISOString(),
    });

    const options = {
      hostname: "n8n",
      port: 5678,
      path: "/webhook/b36e8038-36e8-4dd2-91d3-5414d5b45442",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
    };

    const req = http.request(options, function (res) {
      let data = "";

      res.on("data", function (chunk) {
        data += chunk;
      });

      res.on("end", function () {
        console.log(
          "notify_n8n HTTP:",
          res.statusCode,
          data
        );

        callback(null, [res.statusCode >= 200 && res.statusCode < 300]);
      });
    });

    req.on("error", function (err) {
      console.log("notify_n8n ERROR:", err.message);
      callback(null, [false]);
    });

    req.write(payload);
    req.end();
  },
};