/**
 * @description TMF compliant entity to describes a given characteristic of an object or entity through a name/value pair. 
 */
public with sharing class CharacteristicEntity {
    // A string. Name of the characteristic.
    public String name;
    // A string. The value of the characteristic which needs to be transformed by the data type because JSON deserialization does not support Object
    public String value;
    // A string. Data type of the value of the characteristic.
    public String valueType;

    public CharacteristicEntity() {

    }
}