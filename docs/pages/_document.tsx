import * as React from "react";

import Document, { Head, Main, NextScript } from "next/document";

export default class DefaultDocument extends Document {
  public render() {
    return (
      <html lang="en">
        <Head>
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          <meta property="og:image" content="https://static.dataform.co/og-image.png" />
          <link
            href="https://fonts.googleapis.com/css?family=Roboto+Mono:400,700|Poppins:400,500,600|Open+Sans:400,700|Muli&display=swap"
            rel="stylesheet"
          />
          <link
            rel="stylesheet"
            href="https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css"
          />
          <link
            rel="stylesheet"
            href="/global.css"
          />
          <link rel="shortcut icon" href={"/static/images/favicon.ico"} type="image/png" />
        </Head>
        <body className="light">
          <Main />
          <NextScript />
          {SCRIPTS.map((scriptContent, i) => (
            <script
              key={i}
              dangerouslySetInnerHTML={{
                __html: scriptContent
              }}
            />
          ))}
        </body>
      </html>
    );
  }
}

const SCRIPTS: string[] = [
  // SEGMENT
  `!(function() {
var analytics = (window.analytics = window.analytics || []);
if (!analytics.initialize)
  if (analytics.invoked) window.console && console.error && console.error("Segment snippet included twice.");
  else {
    analytics.invoked = !0;
    analytics.methods = [
      "trackSubmit",
      "trackClick",
      "trackLink",
      "trackForm",
      "pageview",
      "identify",
      "reset",
      "group",
      "track",
      "ready",
      "alias",
      "debug",
      "page",
      "once",
      "off",
      "on"
    ];
    analytics.factory = function(t) {
      return function() {
        var e = Array.prototype.slice.call(arguments);
        e.unshift(t);
        analytics.push(e);
        return analytics;
      };
    };
    for (var t = 0; t < analytics.methods.length; t++) {
      var e = analytics.methods[t];
      analytics[e] = analytics.factory(e);
    }
    analytics.load = function(t) {
      var e = document.createElement("script");
      e.type = "text/javascript";
      e.async = !0;
      e.src =
        ("https:" === document.location.protocol ? "https://" : "http://") +
        "cdn.segment.dataform.co/analytics.js/v1/" +
        t +
        "/analytics.min.js";
      var n = document.getElementsByTagName("script")[0];
      n.parentNode.insertBefore(e, n);
    };
    analytics.SNIPPET_VERSION = "4.0.0";
    analytics.load("zD3lNX5txHQqzJ2HxHVOFPXnBKpqXxUv");
    // Track pageviews on the static site.
    analytics.page();
  }
})();
`
];
