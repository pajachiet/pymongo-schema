
### Database: db0
|Hierarchy                            |Previous Schema              |New Schema                    |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |db0                          |None                          |


### Database: db1
|Hierarchy                            |Previous Schema              |New Schema                    |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |None                         |db1                           |


### Database: db
#### Collection: coll1 
|Hierarchy                            |Previous Schema              |New Schema                    |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |coll1                        |None                          |

#### Collection: coll2 
|Hierarchy                            |Previous Schema              |New Schema                    |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |None                         |coll2                         |

#### Collection: coll 
|Hierarchy                            |Previous Schema              |New Schema                    |
|-------------------------------------|-----------------------------|------------------------------|
|                                     |field2                       |None                          |
|                                     |None                         |field4                        |
|field3                               |{"type": "boolean"}          |{"type": "string"}            |
|field.array_subfield                 |None                         |subsubfield2                  |
|field.array_subfield.subsubfield     |{"type": "integer"}          |{"type": "boolean"}           |
|field5                               |{"array_type": "string"}     |{"array_type": "integer"}     |
|field6                               |{"type": "ARRAY"}            |{"type": "string"}            |

