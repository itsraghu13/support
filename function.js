{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "req",
      "type": "httpTrigger",
      "direction": "in",
      "authLevel": "anonymous",
      "methods": [ "get" ]
    },
    {
      "name": "$return",
      "type": "http",
      "direction": "out"
    }
  ]
}
