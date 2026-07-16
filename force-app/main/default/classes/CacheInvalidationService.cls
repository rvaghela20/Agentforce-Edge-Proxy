public with sharing class CacheInvalidationService {
  public class Request {
    @InvocableVariable(
      required=true
      description='The API name of the updated Salesforce Object (e.g., Product2)'
    )
    public String objectName;

    @InvocableVariable(
      required=true
      description='The Record ID that was created or modified'
    )
    public String recordId;
  }

  public class Response {
    @InvocableVariable(
      description='Indicates if the invalidation command was successfully sent to the edge'
    )
    public Boolean success;
  }

  @InvocableMethod(
    label='Invalidate Edge Cache Key'
    description='Triggers a webhook purge request to the Cloudflare Worker cache'
    callout=true
  )
  public static List<Response> invalidateCache(List<Request> requests) {
    List<Response> responses = new List<Response>();

    for (Request req : requests) {
      Response res = new Response();
      try {
        Map<String, Object> bodyMap = new Map<String, Object>{
          'objectName' => req.objectName,
          'recordId' => req.recordId
        };

        String payload = JSON.serialize(bodyMap);
        String jsonResponse = EdgeProxyService.makePostCallout(
          '/api/invalidate',
          payload
        );

        Map<String, Object> outerMap = (Map<String, Object>) JSON.deserializeUntyped(
          jsonResponse
        );
        res.success = (Boolean) outerMap.get('success');
      } catch (Exception ex) {
        res.success = false;
        System.debug('Cache invalidation callout failed: ' + ex.getMessage());
      }
      responses.add(res);
    }
    return responses;
  }
}
