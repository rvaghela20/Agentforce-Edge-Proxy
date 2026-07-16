public with sharing class EdgeProxyService {
  /**
   * Executes a POST callout to the Cloudflare Worker via Named Credentials.
   * Integrates edge caching, firewalls, and telemetry logging.
   */
  public static String makePostCallout(String path, String payload) {
    Http http = new Http();
    HttpRequest request = new HttpRequest();
    request.setEndpoint('callout:Cloudflare_Edge_Proxy' + path);
    request.setMethod('POST');
    request.setHeader('Content-Type', 'application/json');
    request.setHeader('X-Proxy-Shared-Secret', 'secure-edge-proxy-token-2026');
    request.setTimeout(30000); // Set timeout to 30 seconds for edge callouts
    request.setBody(payload);

    System.debug('Initiating edge callout to: ' + path);
    HttpResponse response = http.send(request);

    if (response.getStatusCode() == 200) {
      System.debug('Callout successful. Response: ' + response.getBody());
      return response.getBody();
    } else {
      System.debug(
        'Callout failed with status: ' +
          response.getStatusCode() +
          ' body: ' +
          response.getBody()
      );
      throw new CalloutException(
        'Cloudflare Worker Error: (' +
          response.getStatusCode() +
          ') ' +
          response.getBody()
      );
    }
  }
}
