package org.community.bigdata.workshop.sparkintro.movielens.model;

/**
 * Contains information about User like id, age, gender, occupation and zip code.
 * Created by tudor on 12/6/2015.
 */
public class User {
    private int id;
    private int age;
    private String gender;
    private String occupation;
    private String zipcode;

    public User(int id, int age, String gender, String occupation, String zipcode) {
        this.id = id;
        this.age = age;
        this.gender = gender;
        this.occupation = occupation;
        this.zipcode = zipcode;
    }

    public int getId() {
        return id;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public String getOccupation() {
        return occupation;
    }

    public String getZipcode() {
        return zipcode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        User user = (User) o;

        if (id != user.id) return false;
        if (age != user.age) return false;
        if (gender != null ? !gender.equals(user.gender) : user.gender != null) return false;
        if (occupation != null ? !occupation.equals(user.occupation) : user.occupation != null) return false;
        return !(zipcode != null ? !zipcode.equals(user.zipcode) : user.zipcode != null);

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + age;
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (occupation != null ? occupation.hashCode() : 0);
        result = 31 * result + (zipcode != null ? zipcode.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", occupation='" + occupation + '\'' +
                ", zipcode='" + zipcode + '\'' +
                '}';
    }
}
