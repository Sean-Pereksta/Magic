// Measured frame rectangles, pivots and collision guides from the supplied art pack.
// The six biome sheets were uploaded directly under platforms/, as confirmed by the owner.
export const DYNAMIC_ASSET_CATALOG = {
  "platform-countryside": {
    "id": "platform-countryside",
    "path": "platforms/countryside-moving-platform.png",
    "kind": "platform",
    "biome": "countryside",
    "width": 2172,
    "height": 724,
    "bytes": 601057,
    "sha256": "04d4c09ecfd713c95bf2f9f695350d825a1293ab68a10489bc8ef193d6f9101f",
    "origin": {
      "x": 199,
      "y": 41
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 199,
          "y": 41
        },
        "contentBounds": {
          "x": 38,
          "y": 26,
          "width": 322,
          "height": 126
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 54,
            "y": 41
          },
          "to": {
            "x": 344,
            "y": 41
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 54,
            "y": 41
          },
          "contactRight": {
            "x": 344,
            "y": 41
          },
          "pivotLeft": {
            "x": 54,
            "y": 41
          },
          "pivotRight": {
            "x": 344,
            "y": 41
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 199,
          "y": 42
        },
        "contentBounds": {
          "x": 38,
          "y": 8,
          "width": 321,
          "height": 144
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 54,
            "y": 42
          },
          "to": {
            "x": 343,
            "y": 42
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 54,
            "y": 42
          },
          "contactRight": {
            "x": 343,
            "y": 42
          },
          "pivotLeft": {
            "x": 54,
            "y": 42
          },
          "pivotRight": {
            "x": 343,
            "y": 42
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 190,
          "y": 40
        },
        "contentBounds": {
          "x": 29,
          "y": 27,
          "width": 321,
          "height": 125
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 45,
            "y": 40
          },
          "to": {
            "x": 334,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 45,
            "y": 40
          },
          "contactRight": {
            "x": 334,
            "y": 40
          },
          "pivotLeft": {
            "x": 45,
            "y": 40
          },
          "pivotRight": {
            "x": 334,
            "y": 40
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1086,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 179,
          "y": 42
        },
        "contentBounds": {
          "x": 20,
          "y": 22,
          "width": 317,
          "height": 130
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 36,
            "y": 42
          },
          "to": {
            "x": 321,
            "y": 42
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 36,
            "y": 42
          },
          "contactRight": {
            "x": 321,
            "y": 42
          },
          "pivotLeft": {
            "x": 36,
            "y": 42
          },
          "pivotRight": {
            "x": 321,
            "y": 42
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1448,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 171,
          "y": 42
        },
        "contentBounds": {
          "x": 11,
          "y": 16,
          "width": 320,
          "height": 136
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 27,
            "y": 42
          },
          "to": {
            "x": 315,
            "y": 42
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 27,
            "y": 42
          },
          "contactRight": {
            "x": 315,
            "y": 42
          },
          "pivotLeft": {
            "x": 27,
            "y": 42
          },
          "pivotRight": {
            "x": 315,
            "y": 42
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 321,
          "width": 362,
          "height": 160
        },
        "origin": {
          "x": 166,
          "y": 42
        },
        "contentBounds": {
          "x": 6,
          "y": 15,
          "width": 319,
          "height": 137
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 22,
            "y": 42
          },
          "to": {
            "x": 309,
            "y": 42
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 22,
            "y": 42
          },
          "contactRight": {
            "x": 309,
            "y": 42
          },
          "pivotLeft": {
            "x": 22,
            "y": 42
          },
          "pivotRight": {
            "x": 309,
            "y": 42
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-desert": {
    "id": "platform-desert",
    "path": "platforms/desert-moving-platform.png",
    "kind": "platform",
    "biome": "desert",
    "width": 2172,
    "height": 724,
    "bytes": 483820,
    "sha256": "8466c2f842d252ad95d01e129c017d1df8fe90b1a3a1352edcb58c29dd27a44b",
    "origin": {
      "x": 199,
      "y": 34
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 199,
          "y": 34
        },
        "contentBounds": {
          "x": 46,
          "y": 25,
          "width": 305,
          "height": 110
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 61,
            "y": 34
          },
          "to": {
            "x": 336,
            "y": 34
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 61,
            "y": 34
          },
          "contactRight": {
            "x": 336,
            "y": 34
          },
          "pivotLeft": {
            "x": 61,
            "y": 34
          },
          "pivotRight": {
            "x": 336,
            "y": 34
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 192,
          "y": 32
        },
        "contentBounds": {
          "x": 40,
          "y": 8,
          "width": 304,
          "height": 127
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 55,
            "y": 32
          },
          "to": {
            "x": 329,
            "y": 32
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 55,
            "y": 32
          },
          "contactRight": {
            "x": 329,
            "y": 32
          },
          "pivotLeft": {
            "x": 55,
            "y": 32
          },
          "pivotRight": {
            "x": 329,
            "y": 32
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 185,
          "y": 33
        },
        "contentBounds": {
          "x": 33,
          "y": 25,
          "width": 304,
          "height": 110
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 48,
            "y": 33
          },
          "to": {
            "x": 322,
            "y": 33
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 48,
            "y": 33
          },
          "contactRight": {
            "x": 322,
            "y": 33
          },
          "pivotLeft": {
            "x": 48,
            "y": 33
          },
          "pivotRight": {
            "x": 322,
            "y": 33
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1086,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 179,
          "y": 33
        },
        "contentBounds": {
          "x": 26,
          "y": 25,
          "width": 305,
          "height": 110
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 41,
            "y": 33
          },
          "to": {
            "x": 316,
            "y": 33
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 41,
            "y": 33
          },
          "contactRight": {
            "x": 316,
            "y": 33
          },
          "pivotLeft": {
            "x": 41,
            "y": 33
          },
          "pivotRight": {
            "x": 316,
            "y": 33
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1448,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 172,
          "y": 33
        },
        "contentBounds": {
          "x": 20,
          "y": 25,
          "width": 304,
          "height": 110
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 35,
            "y": 33
          },
          "to": {
            "x": 309,
            "y": 33
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 35,
            "y": 33
          },
          "contactRight": {
            "x": 309,
            "y": 33
          },
          "pivotLeft": {
            "x": 35,
            "y": 33
          },
          "pivotRight": {
            "x": 309,
            "y": 33
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 309,
          "width": 362,
          "height": 143
        },
        "origin": {
          "x": 166,
          "y": 33
        },
        "contentBounds": {
          "x": 14,
          "y": 25,
          "width": 303,
          "height": 110
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 29,
            "y": 33
          },
          "to": {
            "x": 302,
            "y": 33
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 29,
            "y": 33
          },
          "contactRight": {
            "x": 302,
            "y": 33
          },
          "pivotLeft": {
            "x": 29,
            "y": 33
          },
          "pivotRight": {
            "x": 302,
            "y": 33
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-city": {
    "id": "platform-city",
    "path": "platforms/city-moving-platform.png",
    "kind": "platform",
    "biome": "city",
    "width": 2172,
    "height": 724,
    "bytes": 805551,
    "sha256": "6c7ee150860c4823009a6f0ab8f03b30458d871907e4b08d9b8dbc0a4b54b407",
    "origin": {
      "x": 184,
      "y": 49
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 184,
          "y": 49
        },
        "contentBounds": {
          "x": 16,
          "y": 37,
          "width": 336,
          "height": 192
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 33,
            "y": 49
          },
          "to": {
            "x": 335,
            "y": 49
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 33,
            "y": 49
          },
          "contactRight": {
            "x": 335,
            "y": 49
          },
          "pivotLeft": {
            "x": 33,
            "y": 49
          },
          "pivotRight": {
            "x": 335,
            "y": 49
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 183,
          "y": 48
        },
        "contentBounds": {
          "x": 16,
          "y": 20,
          "width": 334,
          "height": 209
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 33,
            "y": 48
          },
          "to": {
            "x": 333,
            "y": 48
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 33,
            "y": 48
          },
          "contactRight": {
            "x": 333,
            "y": 48
          },
          "pivotLeft": {
            "x": 33,
            "y": 48
          },
          "pivotRight": {
            "x": 333,
            "y": 48
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 183,
          "y": 50
        },
        "contentBounds": {
          "x": 15,
          "y": 9,
          "width": 336,
          "height": 220
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 32,
            "y": 50
          },
          "to": {
            "x": 334,
            "y": 50
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 32,
            "y": 50
          },
          "contactRight": {
            "x": 334,
            "y": 50
          },
          "pivotLeft": {
            "x": 32,
            "y": 50
          },
          "pivotRight": {
            "x": 334,
            "y": 50
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1086,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 182,
          "y": 50
        },
        "contentBounds": {
          "x": 14,
          "y": 8,
          "width": 335,
          "height": 221
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 31,
            "y": 50
          },
          "to": {
            "x": 332,
            "y": 50
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 31,
            "y": 50
          },
          "contactRight": {
            "x": 332,
            "y": 50
          },
          "pivotLeft": {
            "x": 31,
            "y": 50
          },
          "pivotRight": {
            "x": 332,
            "y": 50
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1448,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 183,
          "y": 49
        },
        "contentBounds": {
          "x": 15,
          "y": 27,
          "width": 336,
          "height": 202
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 32,
            "y": 49
          },
          "to": {
            "x": 334,
            "y": 49
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 32,
            "y": 49
          },
          "contactRight": {
            "x": 334,
            "y": 49
          },
          "pivotLeft": {
            "x": 32,
            "y": 49
          },
          "pivotRight": {
            "x": 334,
            "y": 49
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 270,
          "width": 362,
          "height": 237
        },
        "origin": {
          "x": 182,
          "y": 50
        },
        "contentBounds": {
          "x": 14,
          "y": 37,
          "width": 335,
          "height": 192
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 31,
            "y": 50
          },
          "to": {
            "x": 332,
            "y": 50
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 31,
            "y": 50
          },
          "contactRight": {
            "x": 332,
            "y": 50
          },
          "pivotLeft": {
            "x": 31,
            "y": 50
          },
          "pivotRight": {
            "x": 332,
            "y": 50
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-mountains": {
    "id": "platform-mountains",
    "path": "platforms/mountains-moving-platform.png",
    "kind": "platform",
    "biome": "mountains",
    "width": 2171,
    "height": 724,
    "bytes": 430692,
    "sha256": "da72bdd0f2f8a3ce92919781c2e878537603a9794864281a32f5c837caeb87eb",
    "origin": {
      "x": 191,
      "y": 40
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 313,
          "width": 362,
          "height": 128
        },
        "origin": {
          "x": 191,
          "y": 40
        },
        "contentBounds": {
          "x": 31,
          "y": 8,
          "width": 319,
          "height": 112
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 47,
            "y": 40
          },
          "to": {
            "x": 334,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 47,
            "y": 40
          },
          "contactRight": {
            "x": 334,
            "y": 40
          },
          "pivotLeft": {
            "x": 47,
            "y": 40
          },
          "pivotRight": {
            "x": 334,
            "y": 40
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 313,
          "width": 362,
          "height": 128
        },
        "origin": {
          "x": 186,
          "y": 40
        },
        "contentBounds": {
          "x": 28,
          "y": 8,
          "width": 316,
          "height": 111
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 44,
            "y": 40
          },
          "to": {
            "x": 328,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 44,
            "y": 40
          },
          "contactRight": {
            "x": 328,
            "y": 40
          },
          "pivotLeft": {
            "x": 44,
            "y": 40
          },
          "pivotRight": {
            "x": 328,
            "y": 40
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 313,
          "width": 361,
          "height": 128
        },
        "origin": {
          "x": 182,
          "y": 40
        },
        "contentBounds": {
          "x": 24,
          "y": 8,
          "width": 316,
          "height": 111
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 40,
            "y": 40
          },
          "to": {
            "x": 324,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 40,
            "y": 40
          },
          "contactRight": {
            "x": 324,
            "y": 40
          },
          "pivotLeft": {
            "x": 40,
            "y": 40
          },
          "pivotRight": {
            "x": 324,
            "y": 40
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1085,
          "y": 313,
          "width": 362,
          "height": 128
        },
        "origin": {
          "x": 181,
          "y": 40
        },
        "contentBounds": {
          "x": 22,
          "y": 8,
          "width": 317,
          "height": 112
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 38,
            "y": 40
          },
          "to": {
            "x": 323,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 38,
            "y": 40
          },
          "contactRight": {
            "x": 323,
            "y": 40
          },
          "pivotLeft": {
            "x": 38,
            "y": 40
          },
          "pivotRight": {
            "x": 323,
            "y": 40
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1447,
          "y": 313,
          "width": 362,
          "height": 128
        },
        "origin": {
          "x": 177,
          "y": 40
        },
        "contentBounds": {
          "x": 18,
          "y": 8,
          "width": 317,
          "height": 111
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 34,
            "y": 40
          },
          "to": {
            "x": 319,
            "y": 40
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 34,
            "y": 40
          },
          "contactRight": {
            "x": 319,
            "y": 40
          },
          "pivotLeft": {
            "x": 34,
            "y": 40
          },
          "pivotRight": {
            "x": 319,
            "y": 40
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1809,
          "y": 313,
          "width": 362,
          "height": 128
        },
        "origin": {
          "x": 173,
          "y": 40
        },
        "contentBounds": {
          "x": 15,
          "y": 8,
          "width": 316,
          "height": 112
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 31,
            "y": 40
          },
          "to": {
            "x": 315,
            "y": 40
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 31,
            "y": 40
          },
          "contactRight": {
            "x": 315,
            "y": 40
          },
          "pivotLeft": {
            "x": 31,
            "y": 40
          },
          "pivotRight": {
            "x": 315,
            "y": 40
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-ruins": {
    "id": "platform-ruins",
    "path": "platforms/ruins-moving-platform.png",
    "kind": "platform",
    "biome": "ruins",
    "width": 2172,
    "height": 724,
    "bytes": 597679,
    "sha256": "1dcccfbd1b7dbac56805715401a20c09179ec7e0f86b04f17178800c35f3d6d5",
    "origin": {
      "x": 184,
      "y": 15
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 184,
          "y": 15
        },
        "contentBounds": {
          "x": 22,
          "y": 8,
          "width": 324,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 38,
            "y": 15
          },
          "to": {
            "x": 330,
            "y": 15
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 38,
            "y": 15
          },
          "contactRight": {
            "x": 330,
            "y": 15
          },
          "pivotLeft": {
            "x": 38,
            "y": 15
          },
          "pivotRight": {
            "x": 330,
            "y": 15
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 184,
          "y": 15
        },
        "contentBounds": {
          "x": 22,
          "y": 8,
          "width": 324,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 38,
            "y": 15
          },
          "to": {
            "x": 330,
            "y": 15
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 38,
            "y": 15
          },
          "contactRight": {
            "x": 330,
            "y": 15
          },
          "pivotLeft": {
            "x": 38,
            "y": 15
          },
          "pivotRight": {
            "x": 330,
            "y": 15
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 182,
          "y": 15
        },
        "contentBounds": {
          "x": 21,
          "y": 8,
          "width": 322,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 37,
            "y": 15
          },
          "to": {
            "x": 327,
            "y": 15
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 37,
            "y": 15
          },
          "contactRight": {
            "x": 327,
            "y": 15
          },
          "pivotLeft": {
            "x": 37,
            "y": 15
          },
          "pivotRight": {
            "x": 327,
            "y": 15
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1086,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 181,
          "y": 15
        },
        "contentBounds": {
          "x": 19,
          "y": 8,
          "width": 323,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 35,
            "y": 15
          },
          "to": {
            "x": 326,
            "y": 15
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 35,
            "y": 15
          },
          "contactRight": {
            "x": 326,
            "y": 15
          },
          "pivotLeft": {
            "x": 35,
            "y": 15
          },
          "pivotRight": {
            "x": 326,
            "y": 15
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1448,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 180,
          "y": 15
        },
        "contentBounds": {
          "x": 18,
          "y": 8,
          "width": 324,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 34,
            "y": 15
          },
          "to": {
            "x": 326,
            "y": 15
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 34,
            "y": 15
          },
          "contactRight": {
            "x": 326,
            "y": 15
          },
          "pivotLeft": {
            "x": 34,
            "y": 15
          },
          "pivotRight": {
            "x": 326,
            "y": 15
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 324,
          "width": 362,
          "height": 151
        },
        "origin": {
          "x": 180,
          "y": 15
        },
        "contentBounds": {
          "x": 18,
          "y": 8,
          "width": 324,
          "height": 135
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 34,
            "y": 15
          },
          "to": {
            "x": 326,
            "y": 15
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 34,
            "y": 15
          },
          "contactRight": {
            "x": 326,
            "y": 15
          },
          "pivotLeft": {
            "x": 34,
            "y": 15
          },
          "pivotRight": {
            "x": 326,
            "y": 15
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-storm": {
    "id": "platform-storm",
    "path": "platforms/storm-moving-platform.png",
    "kind": "platform",
    "biome": "storm",
    "width": 2172,
    "height": 724,
    "bytes": 597274,
    "sha256": "f490bf96f9fd578b357335287d5c2814c3dbe4d22e7291f56f2d2c77e9030066",
    "origin": {
      "x": 202,
      "y": 13
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 202,
          "y": 13
        },
        "contentBounds": {
          "x": 49,
          "y": 9,
          "width": 306,
          "height": 148
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 64,
            "y": 13
          },
          "to": {
            "x": 340,
            "y": 13
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 64,
            "y": 13
          },
          "contactRight": {
            "x": 340,
            "y": 13
          },
          "pivotLeft": {
            "x": 64,
            "y": 13
          },
          "pivotRight": {
            "x": 340,
            "y": 13
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 140,
        "rect": {
          "x": 362,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 195,
          "y": 13
        },
        "contentBounds": {
          "x": 42,
          "y": 9,
          "width": 306,
          "height": 149
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 57,
            "y": 13
          },
          "to": {
            "x": 333,
            "y": 13
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 57,
            "y": 13
          },
          "contactRight": {
            "x": 333,
            "y": 13
          },
          "pivotLeft": {
            "x": 57,
            "y": 13
          },
          "pivotRight": {
            "x": 333,
            "y": 13
          }
        }
      },
      {
        "index": 2,
        "name": "travel-a",
        "durationMs": 140,
        "rect": {
          "x": 724,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 190,
          "y": 13
        },
        "contentBounds": {
          "x": 35,
          "y": 8,
          "width": 309,
          "height": 149
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 50,
            "y": 13
          },
          "to": {
            "x": 329,
            "y": 13
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 50,
            "y": 13
          },
          "contactRight": {
            "x": 329,
            "y": 13
          },
          "pivotLeft": {
            "x": 50,
            "y": 13
          },
          "pivotRight": {
            "x": 329,
            "y": 13
          }
        }
      },
      {
        "index": 3,
        "name": "travel-b",
        "durationMs": 140,
        "rect": {
          "x": 1086,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 178,
          "y": 13
        },
        "contentBounds": {
          "x": 24,
          "y": 8,
          "width": 307,
          "height": 150
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 39,
            "y": 13
          },
          "to": {
            "x": 316,
            "y": 13
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 39,
            "y": 13
          },
          "contactRight": {
            "x": 316,
            "y": 13
          },
          "pivotLeft": {
            "x": 39,
            "y": 13
          },
          "pivotRight": {
            "x": 316,
            "y": 13
          }
        }
      },
      {
        "index": 4,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 1448,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 169,
          "y": 13
        },
        "contentBounds": {
          "x": 15,
          "y": 9,
          "width": 307,
          "height": 148
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 30,
            "y": 13
          },
          "to": {
            "x": 307,
            "y": 13
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 30,
            "y": 13
          },
          "contactRight": {
            "x": 307,
            "y": 13
          },
          "pivotLeft": {
            "x": 30,
            "y": 13
          },
          "pivotRight": {
            "x": 307,
            "y": 13
          }
        }
      },
      {
        "index": 5,
        "name": "inactive-cue",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 300,
          "width": 362,
          "height": 166
        },
        "origin": {
          "x": 162,
          "y": 13
        },
        "contentBounds": {
          "x": 9,
          "y": 8,
          "width": 306,
          "height": 150
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 24,
            "y": 13
          },
          "to": {
            "x": 300,
            "y": 13
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 24,
            "y": 13
          },
          "contactRight": {
            "x": 300,
            "y": 13
          },
          "pivotLeft": {
            "x": 24,
            "y": 13
          },
          "pivotRight": {
            "x": 300,
            "y": 13
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            140
          ],
          [
            2,
            140
          ],
          [
            3,
            140
          ],
          [
            4,
            900
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 2720
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-a": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ]
        ],
        "durationMs": 140
      },
      "travel-b": {
        "loop": true,
        "frames": [
          [
            3,
            140
          ]
        ],
        "durationMs": 140
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            4,
            900
          ]
        ],
        "durationMs": 900
      },
      "inactive-cue": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      },
      "travel": {
        "loop": true,
        "frames": [
          [
            2,
            140
          ],
          [
            3,
            140
          ]
        ],
        "durationMs": 280
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            1,
            140
          ],
          [
            0,
            700
          ]
        ],
        "durationMs": 840
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "travel-a": 2,
      "travel-b": 3,
      "warning": 4,
      "inactive-cue": 5
    },
    "optional": true
  },
  "platform-collapsing-stone": {
    "id": "platform-collapsing-stone",
    "path": "platforms/collapsing-stone-platform.png",
    "kind": "collapse",
    "width": 2172,
    "height": 724,
    "bytes": 731707,
    "sha256": "e44635d3211a7ce6f85fcd4642c99e44cb0819cb404625c6342d180d9c624657",
    "origin": {
      "x": 194,
      "y": 60
    },
    "defaultAnimation": "stable",
    "frames": [
      {
        "index": 0,
        "name": "stable",
        "durationMs": 1200,
        "rect": {
          "x": 0,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 194,
          "y": 60
        },
        "contentBounds": {
          "x": 36,
          "y": 39,
          "width": 315,
          "height": 172
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 52,
            "y": 60
          },
          "to": {
            "x": 335,
            "y": 60
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 52,
            "y": 60
          },
          "contactRight": {
            "x": 335,
            "y": 60
          },
          "pivotLeft": {
            "x": 52,
            "y": 60
          },
          "pivotRight": {
            "x": 335,
            "y": 60
          }
        }
      },
      {
        "index": 1,
        "name": "warning",
        "durationMs": 500,
        "rect": {
          "x": 362,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 190,
          "y": 57
        },
        "contentBounds": {
          "x": 34,
          "y": 36,
          "width": 312,
          "height": 177
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 50,
            "y": 57
          },
          "to": {
            "x": 330,
            "y": 57
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 50,
            "y": 57
          },
          "contactRight": {
            "x": 330,
            "y": 57
          },
          "pivotLeft": {
            "x": 50,
            "y": 57
          },
          "pivotRight": {
            "x": 330,
            "y": 57
          }
        }
      },
      {
        "index": 2,
        "name": "cracking",
        "durationMs": 300,
        "rect": {
          "x": 724,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 187,
          "y": 59
        },
        "contentBounds": {
          "x": 32,
          "y": 36,
          "width": 309,
          "height": 228
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 47,
            "y": 59
          },
          "to": {
            "x": 326,
            "y": 59
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 47,
            "y": 59
          },
          "contactRight": {
            "x": 326,
            "y": 59
          },
          "pivotLeft": {
            "x": 47,
            "y": 59
          },
          "pivotRight": {
            "x": 326,
            "y": 59
          }
        }
      },
      {
        "index": 3,
        "name": "breaking",
        "durationMs": 180,
        "rect": {
          "x": 1086,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 179,
          "y": 60
        },
        "contentBounds": {
          "x": 21,
          "y": 44,
          "width": 315,
          "height": 241
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 37,
            "y": 60
          },
          "to": {
            "x": 320,
            "y": 60
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 37,
            "y": 60
          },
          "contactRight": {
            "x": 320,
            "y": 60
          },
          "pivotLeft": {
            "x": 37,
            "y": 60
          },
          "pivotRight": {
            "x": 320,
            "y": 60
          }
        }
      },
      {
        "index": 4,
        "name": "fallen",
        "durationMs": 700,
        "rect": {
          "x": 1448,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 173,
          "y": 60
        },
        "contentBounds": {
          "x": 33,
          "y": 55,
          "width": 280,
          "height": 282
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 47,
            "y": 60
          },
          "to": {
            "x": 299,
            "y": 60
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 47,
            "y": 60
          },
          "contactRight": {
            "x": 299,
            "y": 60
          },
          "pivotLeft": {
            "x": 47,
            "y": 60
          },
          "pivotRight": {
            "x": 299,
            "y": 60
          }
        }
      },
      {
        "index": 5,
        "name": "reforming",
        "durationMs": 350,
        "rect": {
          "x": 1810,
          "y": 272,
          "width": 362,
          "height": 345
        },
        "origin": {
          "x": 166,
          "y": 63
        },
        "contentBounds": {
          "x": 11,
          "y": 8,
          "width": 309,
          "height": 212
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 26,
            "y": 63
          },
          "to": {
            "x": 305,
            "y": 63
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 26,
            "y": 63
          },
          "contactRight": {
            "x": 305,
            "y": 63
          },
          "pivotLeft": {
            "x": 26,
            "y": 63
          },
          "pivotRight": {
            "x": 305,
            "y": 63
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            1200
          ],
          [
            1,
            500
          ],
          [
            2,
            300
          ],
          [
            3,
            180
          ],
          [
            4,
            700
          ],
          [
            5,
            350
          ]
        ],
        "durationMs": 3230
      },
      "stable": {
        "loop": true,
        "frames": [
          [
            0,
            1200
          ]
        ],
        "durationMs": 1200
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            1,
            500
          ]
        ],
        "durationMs": 500
      },
      "cracking": {
        "loop": true,
        "frames": [
          [
            2,
            300
          ]
        ],
        "durationMs": 300
      },
      "breaking": {
        "loop": true,
        "frames": [
          [
            3,
            180
          ]
        ],
        "durationMs": 180
      },
      "fallen": {
        "loop": true,
        "frames": [
          [
            4,
            700
          ]
        ],
        "durationMs": 700
      },
      "reforming": {
        "loop": true,
        "frames": [
          [
            5,
            350
          ]
        ],
        "durationMs": 350
      }
    },
    "namedFrames": {
      "stable": 0,
      "warning": 1,
      "cracking": 2,
      "breaking": 3,
      "fallen": 4,
      "reforming": 5
    },
    "optional": true
  },
  "platform-unstable-bridge": {
    "id": "platform-unstable-bridge",
    "path": "platforms/unstable-wooden-bridge.png",
    "kind": "bridge",
    "width": 2172,
    "height": 724,
    "bytes": 489299,
    "sha256": "c303c891f150f16fdf9390d3571db81a2080855955b46a0f62727cce7f482296",
    "origin": {
      "x": 190,
      "y": 61
    },
    "defaultAnimation": "stable",
    "frames": [
      {
        "index": 0,
        "name": "stable",
        "durationMs": 1000,
        "rect": {
          "x": 0,
          "y": 239,
          "width": 362,
          "height": 185
        },
        "origin": {
          "x": 190,
          "y": 61
        },
        "contentBounds": {
          "x": 31,
          "y": 54,
          "width": 318,
          "height": 123
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 47,
            "y": 61
          },
          "to": {
            "x": 333,
            "y": 61
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 47,
            "y": 61
          },
          "contactRight": {
            "x": 333,
            "y": 61
          },
          "pivotLeft": {
            "x": 47,
            "y": 61
          },
          "pivotRight": {
            "x": 333,
            "y": 61
          }
        }
      },
      {
        "index": 1,
        "name": "warning",
        "durationMs": 650,
        "rect": {
          "x": 362,
          "y": 239,
          "width": 362,
          "height": 185
        },
        "origin": {
          "x": 191,
          "y": 61
        },
        "contentBounds": {
          "x": 31,
          "y": 8,
          "width": 319,
          "height": 169
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 47,
            "y": 61
          },
          "to": {
            "x": 334,
            "y": 61
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 47,
            "y": 61
          },
          "contactRight": {
            "x": 334,
            "y": 61
          },
          "pivotLeft": {
            "x": 47,
            "y": 61
          },
          "pivotRight": {
            "x": 334,
            "y": 61
          }
        }
      },
      {
        "index": 2,
        "name": "sagging",
        "durationMs": 300,
        "rect": {
          "x": 724,
          "y": 239,
          "width": 362,
          "height": 185
        },
        "origin": {
          "x": 194,
          "y": 61
        },
        "contentBounds": {
          "x": 32,
          "y": 56,
          "width": 323,
          "height": 121
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 48,
            "y": 61
          },
          "to": {
            "x": 339,
            "y": 61
          },
          "solid": true,
          "note": "Sagging art: keep a conservative flat contact plane until the collision surface is explicitly animated."
        },
        "anchors": {
          "contactLeft": {
            "x": 48,
            "y": 61
          },
          "contactRight": {
            "x": 339,
            "y": 61
          },
          "pivotLeft": {
            "x": 48,
            "y": 61
          },
          "pivotRight": {
            "x": 339,
            "y": 61
          }
        }
      },
      {
        "index": 3,
        "name": "broken",
        "durationMs": 700,
        "rect": {
          "x": 1086,
          "y": 239,
          "width": 364,
          "height": 185
        },
        "origin": {
          "x": 195,
          "y": 61
        },
        "contentBounds": {
          "x": 26,
          "y": 56,
          "width": 338,
          "height": 121
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 43,
            "y": 61
          },
          "to": {
            "x": 347,
            "y": 61
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 43,
            "y": 61
          },
          "contactRight": {
            "x": 347,
            "y": 61
          },
          "pivotLeft": {
            "x": 43,
            "y": 61
          },
          "pivotRight": {
            "x": 347,
            "y": 61
          }
        }
      },
      {
        "index": 4,
        "name": "reforming",
        "durationMs": 400,
        "rect": {
          "x": 1450,
          "y": 239,
          "width": 360,
          "height": 185
        },
        "origin": {
          "x": 188,
          "y": 61
        },
        "contentBounds": {
          "x": 25,
          "y": 9,
          "width": 326,
          "height": 168
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 41,
            "y": 61
          },
          "to": {
            "x": 335,
            "y": 61
          },
          "solid": false
        },
        "anchors": {
          "contactLeft": {
            "x": 41,
            "y": 61
          },
          "contactRight": {
            "x": 335,
            "y": 61
          },
          "pivotLeft": {
            "x": 41,
            "y": 61
          },
          "pivotRight": {
            "x": 335,
            "y": 61
          }
        }
      },
      {
        "index": 5,
        "name": "stable-restored",
        "durationMs": 500,
        "rect": {
          "x": 1810,
          "y": 239,
          "width": 362,
          "height": 185
        },
        "origin": {
          "x": 181,
          "y": 61
        },
        "contentBounds": {
          "x": 25,
          "y": 55,
          "width": 311,
          "height": 122
        },
        "collisionGuide": {
          "type": "oneWaySegment",
          "from": {
            "x": 41,
            "y": 61
          },
          "to": {
            "x": 320,
            "y": 61
          },
          "solid": true
        },
        "anchors": {
          "contactLeft": {
            "x": 41,
            "y": 61
          },
          "contactRight": {
            "x": 320,
            "y": 61
          },
          "pivotLeft": {
            "x": 41,
            "y": 61
          },
          "pivotRight": {
            "x": 320,
            "y": 61
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            1000
          ],
          [
            1,
            650
          ],
          [
            2,
            300
          ],
          [
            3,
            700
          ],
          [
            4,
            400
          ],
          [
            5,
            500
          ]
        ],
        "durationMs": 3550
      },
      "stable": {
        "loop": true,
        "frames": [
          [
            0,
            1000
          ]
        ],
        "durationMs": 1000
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            1,
            650
          ]
        ],
        "durationMs": 650
      },
      "sagging": {
        "loop": true,
        "frames": [
          [
            2,
            300
          ]
        ],
        "durationMs": 300
      },
      "broken": {
        "loop": true,
        "frames": [
          [
            3,
            700
          ]
        ],
        "durationMs": 700
      },
      "reforming": {
        "loop": true,
        "frames": [
          [
            4,
            400
          ]
        ],
        "durationMs": 400
      },
      "stable-restored": {
        "loop": true,
        "frames": [
          [
            5,
            500
          ]
        ],
        "durationMs": 500
      }
    },
    "namedFrames": {
      "stable": 0,
      "warning": 1,
      "sagging": 2,
      "broken": 3,
      "reforming": 4,
      "stable-restored": 5
    },
    "optional": true
  },
  "hazard-pendulum": {
    "id": "hazard-pendulum",
    "path": "hazards/spiked-pendulum-bob.png",
    "kind": "pendulum",
    "width": 2172,
    "height": 724,
    "bytes": 1162028,
    "sha256": "c6f78ce7e24d49d4e0536e26d2e49e1bfefe6e3044f0ef36ff2be1b4a3d3595d",
    "origin": {
      "x": 181,
      "y": 224
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 800,
        "rect": {
          "x": 0,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 181,
          "y": 224
        },
        "contentBounds": {
          "x": 17,
          "y": 8,
          "width": 328,
          "height": 377
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 181,
            "y": 224
          },
          "radius": 150,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 181,
            "y": 224
          },
          "chainAttach": {
            "x": 181,
            "y": 33
          }
        }
      },
      {
        "index": 1,
        "name": "warning-a",
        "durationMs": 450,
        "rect": {
          "x": 362,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 182,
          "y": 225
        },
        "contentBounds": {
          "x": 17,
          "y": 8,
          "width": 329,
          "height": 378
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 182,
            "y": 225
          },
          "radius": 151,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 182,
            "y": 225
          },
          "chainAttach": {
            "x": 182,
            "y": 33
          }
        }
      },
      {
        "index": 2,
        "name": "warning-b",
        "durationMs": 450,
        "rect": {
          "x": 724,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 181,
          "y": 224
        },
        "contentBounds": {
          "x": 17,
          "y": 8,
          "width": 328,
          "height": 377
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 181,
            "y": 224
          },
          "radius": 150,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 181,
            "y": 224
          },
          "chainAttach": {
            "x": 181,
            "y": 33
          }
        }
      },
      {
        "index": 3,
        "name": "active",
        "durationMs": 650,
        "rect": {
          "x": 1086,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 181,
          "y": 225
        },
        "contentBounds": {
          "x": 15,
          "y": 8,
          "width": 331,
          "height": 378
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 181,
            "y": 225
          },
          "radius": 152,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 181,
            "y": 225
          },
          "chainAttach": {
            "x": 181,
            "y": 33
          }
        }
      },
      {
        "index": 4,
        "name": "recovery",
        "durationMs": 350,
        "rect": {
          "x": 1448,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 181,
          "y": 224
        },
        "contentBounds": {
          "x": 16,
          "y": 8,
          "width": 329,
          "height": 377
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 181,
            "y": 224
          },
          "radius": 151,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 181,
            "y": 224
          },
          "chainAttach": {
            "x": 181,
            "y": 33
          }
        }
      },
      {
        "index": 5,
        "name": "idle-restored",
        "durationMs": 800,
        "rect": {
          "x": 1810,
          "y": 163,
          "width": 362,
          "height": 394
        },
        "origin": {
          "x": 181,
          "y": 224
        },
        "contentBounds": {
          "x": 17,
          "y": 8,
          "width": 328,
          "height": 377
        },
        "collisionGuide": {
          "type": "circle",
          "center": {
            "x": 181,
            "y": 224
          },
          "radius": 150,
          "dangerous": true
        },
        "anchors": {
          "bodyCenter": {
            "x": 181,
            "y": 224
          },
          "chainAttach": {
            "x": 181,
            "y": 33
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            800
          ],
          [
            1,
            450
          ],
          [
            2,
            450
          ],
          [
            3,
            650
          ],
          [
            4,
            350
          ],
          [
            5,
            800
          ]
        ],
        "durationMs": 3500
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            800
          ]
        ],
        "durationMs": 800
      },
      "warning-a": {
        "loop": true,
        "frames": [
          [
            1,
            450
          ]
        ],
        "durationMs": 450
      },
      "warning-b": {
        "loop": true,
        "frames": [
          [
            2,
            450
          ]
        ],
        "durationMs": 450
      },
      "active": {
        "loop": true,
        "frames": [
          [
            3,
            650
          ]
        ],
        "durationMs": 650
      },
      "recovery": {
        "loop": true,
        "frames": [
          [
            4,
            350
          ]
        ],
        "durationMs": 350
      },
      "idle-restored": {
        "loop": true,
        "frames": [
          [
            5,
            800
          ]
        ],
        "durationMs": 800
      }
    },
    "namedFrames": {
      "idle": 0,
      "warning-a": 1,
      "warning-b": 2,
      "active": 3,
      "recovery": 4,
      "idle-restored": 5
    },
    "optional": true
  },
  "hazard-crusher": {
    "id": "hazard-crusher",
    "path": "hazards/stone-crusher-head.png",
    "kind": "crusher",
    "width": 2172,
    "height": 724,
    "bytes": 979609,
    "sha256": "2ac3662b35dfb42e92b7bff28f6e20e73d6248829a58ea9404641db641efdef3",
    "origin": {
      "x": 184,
      "y": 258
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 900,
        "rect": {
          "x": 0,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 184,
          "y": 258
        },
        "contentBounds": {
          "x": 28,
          "y": 9,
          "width": 312,
          "height": 249
        },
        "collisionGuide": {
          "type": "rect",
          "x": 44,
          "y": 49,
          "width": 281,
          "height": 199,
          "dangerous": false
        },
        "anchors": {
          "impactCenter": {
            "x": 184,
            "y": 258
          },
          "shaftAttach": {
            "x": 184,
            "y": 29
          }
        }
      },
      {
        "index": 1,
        "name": "warning-a",
        "durationMs": 450,
        "rect": {
          "x": 362,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 183,
          "y": 258
        },
        "contentBounds": {
          "x": 27,
          "y": 9,
          "width": 312,
          "height": 249
        },
        "collisionGuide": {
          "type": "rect",
          "x": 43,
          "y": 49,
          "width": 281,
          "height": 199,
          "dangerous": false
        },
        "anchors": {
          "impactCenter": {
            "x": 183,
            "y": 258
          },
          "shaftAttach": {
            "x": 183,
            "y": 29
          }
        }
      },
      {
        "index": 2,
        "name": "warning-b",
        "durationMs": 450,
        "rect": {
          "x": 724,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 182,
          "y": 258
        },
        "contentBounds": {
          "x": 26,
          "y": 8,
          "width": 312,
          "height": 250
        },
        "collisionGuide": {
          "type": "rect",
          "x": 42,
          "y": 48,
          "width": 281,
          "height": 200,
          "dangerous": false
        },
        "anchors": {
          "impactCenter": {
            "x": 182,
            "y": 258
          },
          "shaftAttach": {
            "x": 182,
            "y": 28
          }
        }
      },
      {
        "index": 3,
        "name": "impact",
        "durationMs": 300,
        "rect": {
          "x": 1086,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 184,
          "y": 258
        },
        "contentBounds": {
          "x": 27,
          "y": 8,
          "width": 314,
          "height": 250
        },
        "collisionGuide": {
          "type": "rect",
          "x": 43,
          "y": 48,
          "width": 283,
          "height": 200,
          "dangerous": true
        },
        "anchors": {
          "impactCenter": {
            "x": 184,
            "y": 258
          },
          "shaftAttach": {
            "x": 184,
            "y": 28
          }
        }
      },
      {
        "index": 4,
        "name": "recovery",
        "durationMs": 400,
        "rect": {
          "x": 1448,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 183,
          "y": 258
        },
        "contentBounds": {
          "x": 27,
          "y": 9,
          "width": 312,
          "height": 249
        },
        "collisionGuide": {
          "type": "rect",
          "x": 43,
          "y": 49,
          "width": 281,
          "height": 199,
          "dangerous": false
        },
        "anchors": {
          "impactCenter": {
            "x": 183,
            "y": 258
          },
          "shaftAttach": {
            "x": 183,
            "y": 29
          }
        }
      },
      {
        "index": 5,
        "name": "idle-restored",
        "durationMs": 900,
        "rect": {
          "x": 1810,
          "y": 224,
          "width": 362,
          "height": 266
        },
        "origin": {
          "x": 184,
          "y": 258
        },
        "contentBounds": {
          "x": 27,
          "y": 8,
          "width": 313,
          "height": 250
        },
        "collisionGuide": {
          "type": "rect",
          "x": 43,
          "y": 48,
          "width": 282,
          "height": 200,
          "dangerous": false
        },
        "anchors": {
          "impactCenter": {
            "x": 184,
            "y": 258
          },
          "shaftAttach": {
            "x": 184,
            "y": 28
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            900
          ],
          [
            1,
            450
          ],
          [
            2,
            450
          ],
          [
            3,
            300
          ],
          [
            4,
            400
          ],
          [
            5,
            900
          ]
        ],
        "durationMs": 3400
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            900
          ]
        ],
        "durationMs": 900
      },
      "warning-a": {
        "loop": true,
        "frames": [
          [
            1,
            450
          ]
        ],
        "durationMs": 450
      },
      "warning-b": {
        "loop": true,
        "frames": [
          [
            2,
            450
          ]
        ],
        "durationMs": 450
      },
      "impact": {
        "loop": true,
        "frames": [
          [
            3,
            300
          ]
        ],
        "durationMs": 300
      },
      "recovery": {
        "loop": true,
        "frames": [
          [
            4,
            400
          ]
        ],
        "durationMs": 400
      },
      "idle-restored": {
        "loop": true,
        "frames": [
          [
            5,
            900
          ]
        ],
        "durationMs": 900
      }
    },
    "namedFrames": {
      "idle": 0,
      "warning-a": 1,
      "warning-b": 2,
      "impact": 3,
      "recovery": 4,
      "idle-restored": 5
    },
    "optional": true
  },
  "hazard-retracting-spikes": {
    "id": "hazard-retracting-spikes",
    "path": "hazards/retracting-spikes.png",
    "kind": "spikes",
    "width": 2172,
    "height": 724,
    "bytes": 541323,
    "sha256": "e898f269f0ce21056c64f01f8c4c0b37279ba91cbcf6b13eafa61e1d6e310321",
    "origin": {
      "x": 200,
      "y": 249
    },
    "defaultAnimation": "retracted",
    "frames": [
      {
        "index": 0,
        "name": "retracted",
        "durationMs": 1100,
        "rect": {
          "x": 0,
          "y": 254,
          "width": 362,
          "height": 257
        },
        "origin": {
          "x": 200,
          "y": 249
        },
        "contentBounds": {
          "x": 37,
          "y": 141,
          "width": 325,
          "height": 108
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": false,
          "polygons": [
            [
              [
                90,
                150
              ],
              [
                117,
                150
              ],
              [
                145,
                150
              ]
            ],
            [
              [
                172,
                150
              ],
              [
                200,
                150
              ],
              [
                227,
                150
              ]
            ],
            [
              [
                254,
                150
              ],
              [
                282,
                150
              ],
              [
                309,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 200,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 117,
              "y": 150
            },
            {
              "x": 200,
              "y": 150
            },
            {
              "x": 282,
              "y": 150
            }
          ]
        }
      },
      {
        "index": 1,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 362,
          "y": 254,
          "width": 362,
          "height": 257
        },
        "origin": {
          "x": 195,
          "y": 249
        },
        "contentBounds": {
          "x": 30,
          "y": 141,
          "width": 330,
          "height": 108
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": false,
          "polygons": [
            [
              [
                84,
                150
              ],
              [
                111,
                150
              ],
              [
                139,
                150
              ]
            ],
            [
              [
                167,
                150
              ],
              [
                195,
                150
              ],
              [
                223,
                150
              ]
            ],
            [
              [
                251,
                150
              ],
              [
                279,
                150
              ],
              [
                306,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 195,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 111,
              "y": 150
            },
            {
              "x": 195,
              "y": 150
            },
            {
              "x": 279,
              "y": 150
            }
          ]
        }
      },
      {
        "index": 2,
        "name": "emerging",
        "durationMs": 180,
        "rect": {
          "x": 724,
          "y": 254,
          "width": 362,
          "height": 257
        },
        "origin": {
          "x": 189,
          "y": 249
        },
        "contentBounds": {
          "x": 26,
          "y": 75,
          "width": 326,
          "height": 174
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": true,
          "polygons": [
            [
              [
                79,
                150
              ],
              [
                106,
                75
              ],
              [
                134,
                150
              ]
            ],
            [
              [
                161,
                150
              ],
              [
                189,
                75
              ],
              [
                217,
                150
              ]
            ],
            [
              [
                244,
                150
              ],
              [
                272,
                75
              ],
              [
                299,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 189,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 106,
              "y": 150
            },
            {
              "x": 189,
              "y": 150
            },
            {
              "x": 272,
              "y": 150
            }
          ]
        }
      },
      {
        "index": 3,
        "name": "extended",
        "durationMs": 650,
        "rect": {
          "x": 1086,
          "y": 254,
          "width": 362,
          "height": 257
        },
        "origin": {
          "x": 177,
          "y": 249
        },
        "contentBounds": {
          "x": 15,
          "y": 8,
          "width": 324,
          "height": 241
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": true,
          "polygons": [
            [
              [
                68,
                150
              ],
              [
                95,
                8
              ],
              [
                122,
                150
              ]
            ],
            [
              [
                150,
                150
              ],
              [
                177,
                8
              ],
              [
                204,
                150
              ]
            ],
            [
              [
                232,
                150
              ],
              [
                259,
                8
              ],
              [
                286,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 177,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 95,
              "y": 150
            },
            {
              "x": 177,
              "y": 150
            },
            {
              "x": 259,
              "y": 150
            }
          ]
        }
      },
      {
        "index": 4,
        "name": "returning",
        "durationMs": 180,
        "rect": {
          "x": 1448,
          "y": 254,
          "width": 361,
          "height": 257
        },
        "origin": {
          "x": 167,
          "y": 249
        },
        "contentBounds": {
          "x": 3,
          "y": 75,
          "width": 328,
          "height": 174
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": true,
          "polygons": [
            [
              [
                56,
                150
              ],
              [
                84,
                75
              ],
              [
                112,
                150
              ]
            ],
            [
              [
                139,
                150
              ],
              [
                167,
                75
              ],
              [
                195,
                150
              ]
            ],
            [
              [
                222,
                150
              ],
              [
                250,
                75
              ],
              [
                278,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 167,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 84,
              "y": 150
            },
            {
              "x": 167,
              "y": 150
            },
            {
              "x": 250,
              "y": 150
            }
          ]
        }
      },
      {
        "index": 5,
        "name": "retracted-restored",
        "durationMs": 800,
        "rect": {
          "x": 1809,
          "y": 254,
          "width": 363,
          "height": 257
        },
        "origin": {
          "x": 165,
          "y": 249
        },
        "contentBounds": {
          "x": 1,
          "y": 141,
          "width": 328,
          "height": 108
        },
        "collisionGuide": {
          "type": "triangles",
          "dangerous": false,
          "polygons": [
            [
              [
                54,
                150
              ],
              [
                82,
                150
              ],
              [
                110,
                150
              ]
            ],
            [
              [
                137,
                150
              ],
              [
                165,
                150
              ],
              [
                193,
                150
              ]
            ],
            [
              [
                220,
                150
              ],
              [
                248,
                150
              ],
              [
                276,
                150
              ]
            ]
          ]
        },
        "anchors": {
          "groundCenter": {
            "x": 165,
            "y": 249
          },
          "slotCenters": [
            {
              "x": 82,
              "y": 150
            },
            {
              "x": 165,
              "y": 150
            },
            {
              "x": 248,
              "y": 150
            }
          ]
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            1100
          ],
          [
            1,
            900
          ],
          [
            2,
            180
          ],
          [
            3,
            650
          ],
          [
            4,
            180
          ],
          [
            5,
            800
          ]
        ],
        "durationMs": 3810
      },
      "retracted": {
        "loop": true,
        "frames": [
          [
            0,
            1100
          ]
        ],
        "durationMs": 1100
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            1,
            900
          ]
        ],
        "durationMs": 900
      },
      "emerging": {
        "loop": true,
        "frames": [
          [
            2,
            180
          ]
        ],
        "durationMs": 180
      },
      "extended": {
        "loop": true,
        "frames": [
          [
            3,
            650
          ]
        ],
        "durationMs": 650
      },
      "returning": {
        "loop": true,
        "frames": [
          [
            4,
            180
          ]
        ],
        "durationMs": 180
      },
      "retracted-restored": {
        "loop": true,
        "frames": [
          [
            5,
            800
          ]
        ],
        "durationMs": 800
      }
    },
    "namedFrames": {
      "retracted": 0,
      "warning": 1,
      "emerging": 2,
      "extended": 3,
      "returning": 4,
      "retracted-restored": 5
    },
    "optional": true
  },
  "hazard-rotating-beam": {
    "id": "hazard-rotating-beam",
    "path": "hazards/rotating-spiked-beam.png",
    "kind": "beam",
    "width": 2172,
    "height": 724,
    "bytes": 487535,
    "sha256": "c852cf95937b14ef19f7c974117b7c6a9e011f4fa0c162ea8564fe64877cc45a",
    "origin": {
      "x": 206,
      "y": 99
    },
    "defaultAnimation": "idle",
    "frames": [
      {
        "index": 0,
        "name": "idle",
        "durationMs": 700,
        "rect": {
          "x": 0,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 206,
          "y": 99
        },
        "contentBounds": {
          "x": 58,
          "y": 8,
          "width": 296,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 70,
            "y": 99
          },
          "to": {
            "x": 342,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 206,
            "y": 99
          }
        }
      },
      {
        "index": 1,
        "name": "activation",
        "durationMs": 180,
        "rect": {
          "x": 362,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 202,
          "y": 99
        },
        "contentBounds": {
          "x": 54,
          "y": 8,
          "width": 296,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 66,
            "y": 99
          },
          "to": {
            "x": 338,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 202,
            "y": 99
          }
        }
      },
      {
        "index": 2,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 724,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 191,
          "y": 99
        },
        "contentBounds": {
          "x": 43,
          "y": 8,
          "width": 295,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 55,
            "y": 99
          },
          "to": {
            "x": 326,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 191,
            "y": 99
          }
        }
      },
      {
        "index": 3,
        "name": "active",
        "durationMs": 650,
        "rect": {
          "x": 1086,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 174,
          "y": 99
        },
        "contentBounds": {
          "x": 26,
          "y": 8,
          "width": 295,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 38,
            "y": 99
          },
          "to": {
            "x": 309,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 174,
            "y": 99
          }
        }
      },
      {
        "index": 4,
        "name": "recovery",
        "durationMs": 350,
        "rect": {
          "x": 1448,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 160,
          "y": 99
        },
        "contentBounds": {
          "x": 12,
          "y": 8,
          "width": 295,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 24,
            "y": 99
          },
          "to": {
            "x": 295,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 160,
            "y": 99
          }
        }
      },
      {
        "index": 5,
        "name": "idle-restored",
        "durationMs": 700,
        "rect": {
          "x": 1810,
          "y": 275,
          "width": 362,
          "height": 198
        },
        "origin": {
          "x": 152,
          "y": 99
        },
        "contentBounds": {
          "x": 4,
          "y": 8,
          "width": 296,
          "height": 182
        },
        "collisionGuide": {
          "type": "capsule",
          "from": {
            "x": 16,
            "y": 99
          },
          "to": {
            "x": 288,
            "y": 99
          },
          "radius": 78,
          "dangerous": true
        },
        "anchors": {
          "rotationPivot": {
            "x": 152,
            "y": 99
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ],
          [
            1,
            180
          ],
          [
            2,
            900
          ],
          [
            3,
            650
          ],
          [
            4,
            350
          ],
          [
            5,
            700
          ]
        ],
        "durationMs": 3480
      },
      "idle": {
        "loop": true,
        "frames": [
          [
            0,
            700
          ]
        ],
        "durationMs": 700
      },
      "activation": {
        "loop": true,
        "frames": [
          [
            1,
            180
          ]
        ],
        "durationMs": 180
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            2,
            900
          ]
        ],
        "durationMs": 900
      },
      "active": {
        "loop": true,
        "frames": [
          [
            3,
            650
          ]
        ],
        "durationMs": 650
      },
      "recovery": {
        "loop": true,
        "frames": [
          [
            4,
            350
          ]
        ],
        "durationMs": 350
      },
      "idle-restored": {
        "loop": true,
        "frames": [
          [
            5,
            700
          ]
        ],
        "durationMs": 700
      }
    },
    "namedFrames": {
      "idle": 0,
      "activation": 1,
      "warning": 2,
      "active": 3,
      "recovery": 4,
      "idle-restored": 5
    },
    "optional": true
  },
  "hazard-timed-gate": {
    "id": "hazard-timed-gate",
    "path": "hazards/timed-stone-gate.png",
    "kind": "gate",
    "width": 2172,
    "height": 724,
    "bytes": 1751404,
    "sha256": "c4f0af47c4f9a866fec29360fed325e103228bf6f5ac0dbb0ebc195ba24d9a09",
    "origin": {
      "x": 185,
      "y": 478
    },
    "defaultAnimation": "closed",
    "frames": [
      {
        "index": 0,
        "name": "closed",
        "durationMs": 900,
        "rect": {
          "x": 0,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 185,
          "y": 478
        },
        "contentBounds": {
          "x": 8,
          "y": 8,
          "width": 353,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 97,
          "y": 140,
          "width": 177,
          "height": 338,
          "solid": true
        },
        "anchors": {
          "groundCenter": {
            "x": 185,
            "y": 478
          }
        }
      },
      {
        "index": 1,
        "name": "warning",
        "durationMs": 900,
        "rect": {
          "x": 362,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 183,
          "y": 478
        },
        "contentBounds": {
          "x": 5,
          "y": 8,
          "width": 355,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 94,
          "y": 140,
          "width": 178,
          "height": 338,
          "solid": true
        },
        "anchors": {
          "groundCenter": {
            "x": 183,
            "y": 478
          }
        }
      },
      {
        "index": 2,
        "name": "opening",
        "durationMs": 250,
        "rect": {
          "x": 724,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 182,
          "y": 478
        },
        "contentBounds": {
          "x": 5,
          "y": 8,
          "width": 353,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 94,
          "y": 140,
          "width": 177,
          "height": 162,
          "solid": true
        },
        "anchors": {
          "groundCenter": {
            "x": 182,
            "y": 478
          }
        }
      },
      {
        "index": 3,
        "name": "open",
        "durationMs": 1800,
        "rect": {
          "x": 1086,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 182,
          "y": 478
        },
        "contentBounds": {
          "x": 4,
          "y": 8,
          "width": 356,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 93,
          "y": 140,
          "width": 178,
          "height": 0,
          "solid": false
        },
        "anchors": {
          "groundCenter": {
            "x": 182,
            "y": 478
          }
        }
      },
      {
        "index": 4,
        "name": "closing",
        "durationMs": 250,
        "rect": {
          "x": 1448,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 182,
          "y": 478
        },
        "contentBounds": {
          "x": 4,
          "y": 8,
          "width": 355,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 93,
          "y": 140,
          "width": 178,
          "height": 162,
          "solid": true
        },
        "anchors": {
          "groundCenter": {
            "x": 182,
            "y": 478
          }
        }
      },
      {
        "index": 5,
        "name": "closed-restored",
        "durationMs": 900,
        "rect": {
          "x": 1810,
          "y": 101,
          "width": 362,
          "height": 486
        },
        "origin": {
          "x": 182,
          "y": 478
        },
        "contentBounds": {
          "x": 5,
          "y": 8,
          "width": 353,
          "height": 470
        },
        "collisionGuide": {
          "type": "gateBlocker",
          "x": 94,
          "y": 140,
          "width": 177,
          "height": 338,
          "solid": true
        },
        "anchors": {
          "groundCenter": {
            "x": 182,
            "y": 478
          }
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            900
          ],
          [
            1,
            900
          ],
          [
            2,
            250
          ],
          [
            3,
            1800
          ],
          [
            4,
            250
          ],
          [
            5,
            900
          ]
        ],
        "durationMs": 5000
      },
      "closed": {
        "loop": true,
        "frames": [
          [
            0,
            900
          ]
        ],
        "durationMs": 900
      },
      "warning": {
        "loop": true,
        "frames": [
          [
            1,
            900
          ]
        ],
        "durationMs": 900
      },
      "opening": {
        "loop": true,
        "frames": [
          [
            2,
            250
          ]
        ],
        "durationMs": 250
      },
      "open": {
        "loop": true,
        "frames": [
          [
            3,
            1800
          ]
        ],
        "durationMs": 1800
      },
      "closing": {
        "loop": true,
        "frames": [
          [
            4,
            250
          ]
        ],
        "durationMs": 250
      },
      "closed-restored": {
        "loop": true,
        "frames": [
          [
            5,
            900
          ]
        ],
        "durationMs": 900
      }
    },
    "namedFrames": {
      "closed": 0,
      "warning": 1,
      "opening": 2,
      "open": 3,
      "closing": 4,
      "closed-restored": 5
    },
    "optional": true
  },
  "fx-platform-activation": {
    "id": "fx-platform-activation",
    "path": "effects/platform-activation.png",
    "kind": "effect",
    "width": 2172,
    "height": 724,
    "bytes": 673623,
    "sha256": "7d0f3846b2fc8a1a4c147c67dbcc52ddffa939bbbfb08d385f92cbd6f7fbbb4d",
    "origin": {
      "x": 189,
      "y": 169
    },
    "defaultAnimation": "activate",
    "frames": [
      {
        "index": 0,
        "name": "spark",
        "durationMs": 70,
        "rect": {
          "x": 0,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 189,
          "y": 169
        },
        "contentBounds": {
          "x": 128,
          "y": 99,
          "width": 122,
          "height": 139
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 1,
        "name": "small-ring",
        "durationMs": 80,
        "rect": {
          "x": 362,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 183,
          "y": 166
        },
        "contentBounds": {
          "x": 77,
          "y": 61,
          "width": 211,
          "height": 209
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 2,
        "name": "growing-ring",
        "durationMs": 90,
        "rect": {
          "x": 724,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 168,
          "y": 164
        },
        "contentBounds": {
          "x": 25,
          "y": 23,
          "width": 286,
          "height": 281
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 3,
        "name": "full-pulse",
        "durationMs": 110,
        "rect": {
          "x": 1086,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 180,
          "y": 167
        },
        "contentBounds": {
          "x": 16,
          "y": 8,
          "width": 328,
          "height": 318
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 4,
        "name": "fading-ring",
        "durationMs": 110,
        "rect": {
          "x": 1448,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 186,
          "y": 170
        },
        "contentBounds": {
          "x": 65,
          "y": 52,
          "width": 242,
          "height": 236
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 5,
        "name": "motes",
        "durationMs": 140,
        "rect": {
          "x": 1810,
          "y": 194,
          "width": 362,
          "height": 334
        },
        "origin": {
          "x": 181,
          "y": 187
        },
        "contentBounds": {
          "x": 87,
          "y": 100,
          "width": 188,
          "height": 173
        },
        "collisionGuide": {
          "type": "none"
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            70
          ],
          [
            1,
            80
          ],
          [
            2,
            90
          ],
          [
            3,
            110
          ],
          [
            4,
            110
          ],
          [
            5,
            140
          ]
        ],
        "durationMs": 600
      },
      "spark": {
        "loop": false,
        "frames": [
          [
            0,
            70
          ]
        ],
        "durationMs": 70
      },
      "small-ring": {
        "loop": false,
        "frames": [
          [
            1,
            80
          ]
        ],
        "durationMs": 80
      },
      "growing-ring": {
        "loop": false,
        "frames": [
          [
            2,
            90
          ]
        ],
        "durationMs": 90
      },
      "full-pulse": {
        "loop": false,
        "frames": [
          [
            3,
            110
          ]
        ],
        "durationMs": 110
      },
      "fading-ring": {
        "loop": false,
        "frames": [
          [
            4,
            110
          ]
        ],
        "durationMs": 110
      },
      "motes": {
        "loop": false,
        "frames": [
          [
            5,
            140
          ]
        ],
        "durationMs": 140
      },
      "activate": {
        "loop": false,
        "frames": [
          [
            0,
            70
          ],
          [
            1,
            80
          ],
          [
            2,
            90
          ],
          [
            3,
            110
          ],
          [
            4,
            110
          ],
          [
            5,
            140
          ]
        ],
        "durationMs": 600
      }
    },
    "namedFrames": {
      "spark": 0,
      "small-ring": 1,
      "growing-ring": 2,
      "full-pulse": 3,
      "fading-ring": 4,
      "motes": 5
    },
    "optional": true
  },
  "ui-navigation": {
    "id": "ui-navigation",
    "path": "ui/navigation-indicators.png",
    "kind": "navigation",
    "width": 2172,
    "height": 724,
    "bytes": 990365,
    "sha256": "3812d58be1c3a49d3762124ab305438c3261aa899160a37f9d1deddad0029554",
    "origin": {
      "x": 302,
      "y": 249
    },
    "defaultAnimation": "move-right",
    "frames": [
      {
        "index": 0,
        "name": "move-right",
        "durationMs": 1000,
        "rect": {
          "x": 0,
          "y": 102,
          "width": 555,
          "height": 487
        },
        "origin": {
          "x": 302,
          "y": 249
        },
        "contentBounds": {
          "x": 48,
          "y": 38,
          "width": 507,
          "height": 421
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 1,
        "name": "move-up",
        "durationMs": 1000,
        "rect": {
          "x": 555,
          "y": 102,
          "width": 531,
          "height": 487
        },
        "origin": {
          "x": 287,
          "y": 241
        },
        "contentBounds": {
          "x": 82,
          "y": 13,
          "width": 409,
          "height": 455
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 2,
        "name": "wait",
        "durationMs": 1000,
        "rect": {
          "x": 1086,
          "y": 102,
          "width": 543,
          "height": 487
        },
        "origin": {
          "x": 273,
          "y": 244
        },
        "contentBounds": {
          "x": 109,
          "y": 8,
          "width": 328,
          "height": 471
        },
        "collisionGuide": {
          "type": "none"
        }
      },
      {
        "index": 3,
        "name": "safe-landing",
        "durationMs": 1000,
        "rect": {
          "x": 1629,
          "y": 102,
          "width": 543,
          "height": 487
        },
        "origin": {
          "x": 271,
          "y": 266
        },
        "contentBounds": {
          "x": 32,
          "y": 84,
          "width": 477,
          "height": 363
        },
        "collisionGuide": {
          "type": "none"
        }
      }
    ],
    "animations": {
      "preview": {
        "loop": true,
        "frames": [
          [
            0,
            1000
          ],
          [
            1,
            1000
          ],
          [
            2,
            1000
          ],
          [
            3,
            1000
          ]
        ],
        "durationMs": 4000
      },
      "move-right": {
        "loop": true,
        "frames": [
          [
            0,
            1000
          ]
        ],
        "durationMs": 1000
      },
      "move-up": {
        "loop": true,
        "frames": [
          [
            1,
            1000
          ]
        ],
        "durationMs": 1000
      },
      "wait": {
        "loop": true,
        "frames": [
          [
            2,
            1000
          ]
        ],
        "durationMs": 1000
      },
      "safe-landing": {
        "loop": true,
        "frames": [
          [
            3,
            1000
          ]
        ],
        "durationMs": 1000
      }
    },
    "namedFrames": {
      "move-right": 0,
      "move-up": 1,
      "wait": 2,
      "safe-landing": 3
    },
    "optional": true
  }
};
export const DYNAMIC_HAZARD_ART = {"pendulum":"hazard-pendulum","crusher":"hazard-crusher","retractSpikes":"hazard-retracting-spikes","rotatingBeam":"hazard-rotating-beam","timedGate":"hazard-timed-gate"};
