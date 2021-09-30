#!/usr/bin/python3
from aioflureedb import FlureeDomainAPI
domain_api = FlureeDomainAPI("./api_maps")
role = domain_api.get_api_by_role("demo_role")
trans1 = role.create_demo_user_role()
print(trans1())
trans2 = role.create_demo_user(full_name="John Doe", email="j.f.doe@gmail.com", pubkey="TfB5z166pcmReVA3sfEqisjgv7pX2gefff0")
print(trans2())
trans3 = role.create_demo_user(full_name="Jane Doe", email="j.e.m.doe@gmail.com", pubkey="TfB5z166pcmReVA3sfEqisjgv7pX2ge0000")
print(trans3())
query = role.get_demo_users()
print(query())
fake_response = [
  [
    {
      "username": "John Doe",
      "_id": 87960930223083,
      "auth": [
        {
          "id": "TfB5z166pcmReVA3sfEqisjgv7pX2gefff0",
          "_id": 105553116267499
        }
      ],
      "doc": "j.f.doe@gmail.com"
    }
  ],
  [
    {
      "username": "Jane Doe",
      "_id": 87960930223082,
      "auth": [
        {
          "id": "TfB5z166pcmReVA3sfEqisjgv7pX2ge0000",
          "_id": 105553116267498
        }
      ],
      "doc": "j.e.m.doe@gmail.com"
    }
  ]
]
print(query(fake_response))
