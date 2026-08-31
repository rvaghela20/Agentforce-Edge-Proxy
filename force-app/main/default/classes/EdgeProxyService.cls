public with sharing class EdgeProxyService {
  /**
   * POST callout to the Cloudflare Worker via Named Credentials.
   */
  public static String makePostCallout(String path, String payload) {
    Http http = new Http();
    HttpRequest request = new HttpRequest();
    request.setEndpoint('callout:Cloudflare_Edge_Proxy' + path);
    request.setMethod('POST');
    request.setHeader('Content-Type', 'application/json');

    // Pull the shared secret from a Custom Setting instead of hardcoding it.
    // Set this in Setup > Custom Settings > Edge Proxy Settings.
    EdgeProxySettings__c settings = EdgeProxySettings__c.getOrgDefaults();
    String secret = settings != null ? settings.Shared_Secret__c : null;
    if (String.isBlank(secret)) {
      throw new CalloutException(
        'Edge proxy shared secret is not configured. Set it in EdgeProxySettings__c.'
      );
    }
    request.setHeader('X-Proxy-Shared-Secret', secret);
    request.setTimeout(30000);
    request.setBody(payload);

    HttpResponse response = http.send(request);

    if (response.getStatusCode() == 200) {
      return response.getBody();
    } else {
      throw new CalloutException(
        'Cloudflare Worker Error: (' +
          response.getStatusCode() +
          ') ' +
          response.getBody()
      );
    }
  }
}

