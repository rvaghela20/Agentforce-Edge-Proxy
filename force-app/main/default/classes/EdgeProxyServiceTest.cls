@isTest
public class EdgeProxyServiceTest {
  // Mock HTTP responder to simulate Cloudflare Workers responses
  public class MockHttpResponseGenerator implements HttpCalloutMock {
    private String path;

    public MockHttpResponseGenerator(String path) {
      this.path = path;
    }

    public HTTPResponse respond(HTTPRequest req) {
      HTTPResponse res = new HTTPResponse();
      res.setHeader('Content-Type', 'application/json');
      res.setStatusCode(200);

      if (this.path == '/api/search') {
        res.setBody(
          '{"data": [{"name": "Mock Test Product", "price": "$100"}], "meta": {"cacheStatus": "HIT", "latencyMs": 4}}'
        );
      } else if (this.path == '/api/telemetry') {
        res.setBody('{"success": true}');
      } else if (this.path == '/api/invalidate') {
        res.setBody('{"success": true}');
      } else {
        res.setBody('{"status": "healthy"}');
      }
      return res;
    }
  }

  @isTest
  static void testSearchActionSuccess() {
    Test.setMock(
      HttpCalloutMock.class,
      new MockHttpResponseGenerator('/api/search')
    );

    AgentSearchAction.Request req = new AgentSearchAction.Request();
    req.query = 'pricing';

    Test.startTest();
    List<AgentSearchAction.Response> responses = AgentSearchAction.searchCRM(
      new List<AgentSearchAction.Request>{ req }
    );
    Test.stopTest();

    System.assertEquals(1, responses.size());
    System.assertEquals('HIT', responses[0].cacheStatus);
    System.assertEquals(4, responses[0].latencyMs);
    System.assert(responses[0].apiResult.contains('Mock Test Product'));
  }

  @isTest
  static void testTelemetryLoggerSuccess() {
    Test.setMock(
      HttpCalloutMock.class,
      new MockHttpResponseGenerator('/api/telemetry')
    );

    TelemetryLoggerAction.Request req = new TelemetryLoggerAction.Request();
    req.promptTokens = 150;
    req.completionTokens = 80;

    Test.startTest();
    List<TelemetryLoggerAction.Response> responses = TelemetryLoggerAction.logTelemetry(
      new List<TelemetryLoggerAction.Request>{ req }
    );
    Test.stopTest();

    System.assertEquals(1, responses.size());
    System.assertEquals(true, responses[0].success);
  }

  @isTest
  static void testCacheInvalidationSuccess() {
    Test.setMock(
      HttpCalloutMock.class,
      new MockHttpResponseGenerator('/api/invalidate')
    );

    CacheInvalidationService.Request req = new CacheInvalidationService.Request();
    req.objectName = 'Product2';
    req.recordId = '01t8W000006GxxxQAD';

    Test.startTest();
    List<CacheInvalidationService.Response> responses = CacheInvalidationService.invalidateCache(
      new List<CacheInvalidationService.Request>{ req }
    );
    Test.stopTest();

    System.assertEquals(1, responses.size());
    System.assertEquals(true, responses[0].success);
  }

  @isTest
  static void testCalloutFailure() {
    // Force an error response (e.g. 500 Internal Server Error)
    HTTPResponse errRes = new HTTPResponse();
    errRes.setStatusCode(500);
    errRes.setBody('Internal Server Error');

    Test.setMock(HttpCalloutMock.class, new MockErrorCalloutMock(errRes));

    Test.startTest();
    try {
      EdgeProxyService.makePostCallout('/api/search', '{"query": "error"}');
      System.assert(false, 'Expected CalloutException to be thrown');
    } catch (CalloutException ex) {
      System.assert(
        ex.getMessage().contains('500'),
        'Error message should include status code'
      );
    }
    Test.stopTest();
  }

  // Auxiliary class to handle custom error HTTP responses
  private class MockErrorCalloutMock implements HttpCalloutMock {
    private HTTPResponse response;
    public MockErrorCalloutMock(HTTPResponse response) {
      this.response = response;
    }
    public HTTPResponse respond(HTTPRequest req) {
      return this.response;
    }
  }
}
