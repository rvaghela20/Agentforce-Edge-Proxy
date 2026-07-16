public with sharing class AgentSearchAction {
  public class Request {
    @InvocableVariable(
      required=true
      description='The search query or keyword to lookup in CRM databases'
    )
    public String query;
  }

  public class Response {
    @InvocableVariable(
      description='The JSON-serialized data returned from search'
    )
    public String apiResult;

    @InvocableVariable(
      description='Cache hit or miss status from the edge proxy'
    )
    public String cacheStatus;

    @InvocableVariable(
      description='Execution time in milliseconds recorded by the proxy'
    )
    public Integer latencyMs;
  }

  @InvocableMethod(
    label='Perform Intelligent Edge Search'
    description='Searches products, FAQs, and SLA documentation via the Cloudflare Worker caching proxy'
    callout=true
  )
  public static List<Response> searchCRM(List<Request> requests) {
    List<Response> responses = new List<Response>();

    for (Request req : requests) {
      Response res = new Response();
      try {
        // Serialize request body to send to Cloudflare Worker
        Map<String, Object> bodyMap = new Map<String, Object>{
          'query' => req.query
        };
        String payload = JSON.serialize(bodyMap);

        // Call Cloudflare Edge Proxy
        String jsonResponse = EdgeProxyService.makePostCallout(
          '/api/search',
          payload
        );

        // Parse returned payload
        Map<String, Object> outerMap = (Map<String, Object>) JSON.deserializeUntyped(
          jsonResponse
        );
        Map<String, Object> metaMap = (Map<String, Object>) outerMap.get(
          'meta'
        );

        res.apiResult = JSON.serialize(outerMap.get('data'));
        res.cacheStatus = (String) metaMap.get('cacheStatus');
        res.latencyMs = (Integer) metaMap.get('latencyMs');
      } catch (Exception ex) {
        res.apiResult = '{"error": "' + ex.getMessage() + '"}';
        res.cacheStatus = 'ERROR';
        res.latencyMs = 0;
      }
      responses.add(res);
    }
    return responses;
  }
}
