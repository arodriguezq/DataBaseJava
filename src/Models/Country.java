package Models;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Country {

    public int country_id;
    public String name;
    private Set<City> hsCities = new HashSet<City>();

    public Country(int country_id, String name) {
        this.country_id = country_id;
        this.name = name;
    }

    public int getCountry_id() {
        return country_id;
    }

    public void setCountry_id(int country_id) {
        this.country_id = country_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<City> getHsCities() {
        return hsCities;
    }

    public void setHsCities(Set<City> hsCities) {
        this.hsCities = hsCities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Country country = (Country) o;

        return country_id == country.country_id;
    }

    @Override
    public int hashCode() {
        return country_id;
    }

    @Override
    public String toString() {
        List<String> cityList = new ArrayList<>();
        List<String> cityidList = new ArrayList<>();
        for(City city: hsCities){
            cityList.add(city.getName());
            cityidList.add(String.valueOf(city.getCity_id()));

        }
        return "Models.Country{" +
                "country_id=" + country_id +
                ", name='" + name +
                ", cities="+ cityidList+ " "+cityList+
                '}';
    }
    public boolean addCity(City c){
        return hsCities.add(c);
    }

    public boolean removeCity(City c){
        return hsCities.remove(c);
    }
}
