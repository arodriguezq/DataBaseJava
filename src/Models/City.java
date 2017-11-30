package Models;

public class City {

    public int city_id;
    public String name;
    public Country country;

    public City(int city_id, String name, Country country) {
        this.city_id = city_id;
        this.name = name;
        this.country = country;
    }

    public int getCity_id() {
        return city_id;
    }

    public void setCity_id(int city_id) {
        this.city_id = city_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        City city = (City) o;

        if (city_id != city.city_id) return false;
        if (name != null ? !name.equals(city.name) : city.name != null) return false;
        return country != null ? country.equals(city.country) : city.country == null;
    }

    @Override
    public int hashCode() {
        int result = city_id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Models.City{" +
                "city_id=" + city_id +
                ", name='" + name + '\'' +
                ", country=" + country +
                '}';
    }
}
