// import ballerina/log;
// import ballerina/http;
// import ballerina/jwt;

// http:JwtValidatorConfig config = {
//     audience: "[<client-id1>, <client-id2>]",
//     signatureConfig: {
//         trustStoreConfig: {
//             trustStore: {
//                 path: "<trust-store-path>",
//                 password: "<trust-store-password>"
//             },
//             certAlias: "<trust-store-alias>"
//         }
//     }
// };
// http:ListenerJwtAuthHandler handler = new(config);

// function authorize(http:Headers headers, string[] authScopes) returns error? {
//     string|http:HeaderNotFoundError authHeader = headers.getHeader(http:AUTH_HEADER);
//     if (authHeader is string) {
//         jwt:Payload|http:Unauthorized auth = handler.authenticate(authHeader);
//         if (auth is jwt:Payload) {
//             http:Forbidden? forbiddenError = handler.authorize(auth, authScopes);
//             if (forbiddenError is http:Forbidden) {
//                 log:printError("Authentication credentials invalid");
//                 return error("Not authorized");
//             }
//         } else {
//             log:printError("Authentication credentials invalid");
//             return error("Not authorized");
//         }
//     } else {
//         log:printError("Authorization header not found");
//         return error("Not authorized");
//     }
// }
