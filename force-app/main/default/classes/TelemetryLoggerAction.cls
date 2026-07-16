public with sharing class TelemetryLoggerAction {
  public class Request {
    @InvocableVariable(
      description='The number of input tokens used in the prompt'
    )
    public Integer promptTokens;

    @InvocableVariable(description='The number of output tokens generated')
    public Integer completionTokens;
  }

  public class Response {
    @InvocableVariable(
      description='Indicates if telemetry was successfully logged'
    )
    public Boolean success;
  }

  @InvocableMethod(
    label='Log Session Telemetry'
    description='Reports prompt costs and tokens back to the Cloudflare analytics database'
    callout=true
  )
  public static List<Response> logTelemetry(List<Request> requests) {
    List<Response> responses = new List<Response>();

    for (Request req : requests) {
      Response res = new Response();
      try {
        Map<String, Object> bodyMap = new Map<String, Object>{
          'promptTokens' => req.promptTokens != null ? req.promptTokens : 0,
          'completionTokens' => req.completionTokens != null
            ? req.completionTokens
            : 0
        };

        String payload = JSON.serialize(bodyMap);
        String jsonResponse = EdgeProxyService.makePostCallout(
          '/api/telemetry',
          payload
        );

        Map<String, Object> outerMap = (Map<String, Object>) JSON.deserializeUntyped(
          jsonResponse
        );
        res.success = (Boolean) outerMap.get('success');
      } catch (Exception ex) {
        res.success = false;
        System.debug('Telemetry logging failed: ' + ex.getMessage());
      }
      responses.add(res);
    }
    return responses;
  }
}
