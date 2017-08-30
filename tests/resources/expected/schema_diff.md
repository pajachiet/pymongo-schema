
### Database: db0
|Hierarchy                            |In Schema                    |In Expected                   |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |db0                          |None                          |


### Database: db1
|Hierarchy                            |In Schema                    |In Expected                   |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |None                         |db1                           |


### Database: db
#### Collection: coll1 
|Hierarchy                            |In Schema                    |In Expected                   |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |coll1                        |None                          |

#### Collection: coll2 
|Hierarchy                            |In Schema                    |In Expected                   |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |None                         |coll2                         |

#### Collection: coll 
|Hierarchy                            |In Schema                    |In Expected                   |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |field2                       |None                          |
|                                     |None                         |field4                        |
|field3                               |{'type': 'boolean'}          |{'type': 'string'}            |
|field.array_subfield                 |None                         |subsubfield2                  |
|field.array_subfield.subsubfield     |{'type': 'integer'}          |{'type': 'boolean'}           |
|field5                               |{'array_type': 'string'}     |{'array_type': 'integer'}     |
|field6                               |{'type': 'ARRAY'}            |{'type': 'string'}            |

