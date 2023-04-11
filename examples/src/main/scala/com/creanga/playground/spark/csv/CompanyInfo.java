package com.creanga.playground.spark.csv;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CompanyInfo implements Serializable {
    private String domain;
    private List<String> name = new ArrayList<>();
    private List<String> address = new ArrayList<>();
    private List<String> city = new ArrayList<>();
    private List<String> country = new ArrayList<>();
    private List<String> region = new ArrayList<>();
    private List<String> phone = new ArrayList<>();
    private List<String> categories = new ArrayList<>();
    private List<String> origin = new ArrayList<>();

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getName() {
        return name.isEmpty()?null:name.get(0);
    }

    public void setName(String name) {
        this.name.add(name);
    }

    public String getAddress() {
        return address.isEmpty()?null:address.get(0);
    }

    public void setAddress(String address) {
        this.address.add(address);
    }

    public String getCity() {
        return city.isEmpty()?null:city.get(0);
    }

    public void setCity(String city) {
        this.city.add(city);
    }

    public String getCountry() {
        return country.isEmpty()?null:country.get(0);
    }

    public void setCountry(String country) {
        this.country.add(country);
    }

    public String getRegion() {
        return region.isEmpty()?null:region.get(0);
    }

    public void setRegion(String region) {
        this.region.add(region);
    }

    public String getPhone() {
        return phone.isEmpty()?null:phone.get(0);
    }

    public void setPhone(String phone) {
        this.phone.add(phone);
    }

    public String getCategories() {
        return categories.isEmpty()?null:categories.get(0);
    }

    public void setCategories(String categories) {
        this.categories.add(categories);
    }

    public String getOrigin() {
        return origin.isEmpty()?null:origin.get(0);
    }

    public void setOrigin(String origin) {
        this.origin.add(origin);
    }
}
