const express = require("express");
const httpProxy = require("http-proxy");

const app = express();
const PORT = 8000;
const BASE_PATH =
  "https://vercel-clone-sudarshan.s3.eu-north-1.amazonaws.com/__outputs";
const proxy = httpProxy.createProxy();
app.use((req, res) => {
  const hostname = req.hostname;
  /**
   * what happening ??
   * first we getting a1.localhost from the request
   * then it goes to s3 a1 -> index.html
   * and it redirect to a1.local host it is reverse proxy
   * request -> reverse proxy -> going to s3 -> getting res -> and showing in a1.localhost
   */
  const subdomain = hostname.split(".")[0]; //a1
  const resolveTo = `${BASE_PATH}/${subdomain}`;
  return proxy.web(req, res, { target: resolveTo, changeOrigin: true });
});

proxy.on("proxyReq", (proxyReq, req, res) => {
  const url = req.url;
  // for this like a1.localhost.index.hmtl ! just like a1.localhost.
  if (url == "/") proxyReq.path += "index.html";
  return proxyReq;
});

app.listen(PORT, () => {
  console.log(`Reverse Proxy Running... ${PORT}`);
});
