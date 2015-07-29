mixpanel-sforce-zap
============
Library to connect Salesforce and Mixpanel

Install
-------
Clone repository, cd into directory and run:
```
$ pip install -r requirements.txt
```

Test
----
Run unit tests:
```
python tests.py
```
or with coverage
```
coverage run --source src tests.py
coverage report -m 
```

Run
---
To run example script:
```
python main.py <args>
```

Arguments include all the keys needed for SFDC and MP Api and list of events
Can be found by running 

```
python main.py -h
```

