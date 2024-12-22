{
  "name": "pattern",
  "quantifier": {
    "consumingStrategy": "SKIP_TILL_NEXT",
    "properties": [
      "SINGLE"
    ]
  },
  "condition": null,
  "nodes": [
    {
      "name": "start",
      "quantifier": {
        "consumingStrategy": "STRICT",
        "innerConsumingStrategy": "SKIP_TILL_NEXT",
        "properties": [
          "SINGLE"
        ]
      },
      "condition": {
        "expression": "paymentAmount > 10",
        "type": "AVIATOR"
      },
      "type": "ATOMIC",
      "afterMatchSkipStrategy": {
        "type": "NO_SKIP",
        "patternName": null
      }
    }
  ],
  "edges": [
  ],
  "window": null,
  "afterMatchStrategy": {
    "type": "SKIP_PAST_LAST_EVENT",
    "patternName": null
  },
  "type": "COMPOSITE",
  "version": 2
}