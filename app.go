package main

import (
	"encoding/json"
	"github.com/drone/routes"
	"github.com/mkilling/goejdb"
	"github.com/naoina/toml"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

type tomlConfig struct {
	Database struct {
		File_name string
		Port_num  int
	}
	Replication struct {
		Rpc_server_port_num int
		Replica             []string
	}
}

type Request struct {
	Email          string `bson:"email"`
	Zip            string `bson:"zip"`
	Country        string `bson:"country"`
	Profession     string `bson:"profession"`
	Favorite_color string `bson:"favorite_color"`
	Is_smoking     string `bson:"is_smoking"`
	Favorite_sport string `bson:"favorite_sport"`
	Food           struct {
		Type     string `bson:"type"`
		Drink_alcohol string `bson:"drink_alcohol"`
	} `bson:"food"`
	Music struct {
		Spotify_user_id string `bson:"spotify_user_id"`
	} `bson:"music"`
	Movie struct {
		Tv_shows [3]string `bson:"tv_shows"`
		Movies   [3]string `bson:"movies"`
	} `bson:"movie"`
	Travel struct {
		Flight struct {
			Seat string `bson:"seat"`
		} `bson:"flight"`
	} `bson:"travel"`
}



type Listener int

var config tomlConfig
var coll *goejdb.EjColl

func (l *Listener) CreateRep(line []byte, ack *bool) error {
	log.Println("Create Replica server")
	coll.SaveBson(line)
	return nil
}
func (l *Listener) DeleteRep(line []byte, ack *bool) error {
	log.Println("Delete Replica server")
	email := string(line)
	coll.Update(`{"email": "` + email + `", "$dropall" : true}`)
	return nil
}
func (l *Listener) UpdateRep(line []byte, ack *bool) error {
	log.Println("Update Replica server")
	p := &Request{}
	err := json.Unmarshal([]byte(line), &p)
	if err == nil {
		var query = `{"email": "` + p.Email + `", "$set":` + string(line) + `}`
		coll.Update(query)
	}
	return nil
}

func RPCServer() {
	
	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+strconv.Itoa(config.Replication.Rpc_server_port_num))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(inbound)
}

func ConfigFile() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	if err := toml.Unmarshal(buf, &config); err != nil {
		panic(err)
	}
	for i := 0; i < len(config.Replication.Replica); i++ {
		config.Replication.Replica[i] = strings.TrimLeft(config.Replication.Replica[i], "http://")
	}
}

func PostProfile(w http.ResponseWriter, r *http.Request) {
	log.Println("Create Profile")
	body, _ := ioutil.ReadAll(r.Body)
	
	prof := &Request{}
	err := json.Unmarshal([]byte(body), &prof)
	var bsrec []byte
	if err != nil {
		panic(err)
		w.Write([]byte(err.Error()))

	} else {
		
		bsrec, _ = bson.Marshal(prof)
		coll.SaveBson(bsrec)
		w.WriteHeader(http.StatusCreated)
	}
	

	for _, each := range config.Replication.Replica {
		client, err := rpc.Dial("tcp", each)
		if err != nil {
			log.Fatal(err)
		}
		var flag bool
		err = client.Call("Listener.CreateRep", bsrec, &flag)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func GetProfile(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	email := params.Get(":email")
	
	query := `{"email": "` + email + `"}`
	log.Println(query)
	res, _ := coll.Find(query)
	log.Println(len(res))
	if len(res) > 0 {
		prof := &Request{}
		bson.Unmarshal(res[0], &prof)
		log.Println(prof.Country)
		profile, err := json.Marshal(prof)
		if err != nil {
			panic(err)
			w.Write([]byte(err.Error()))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(profile)
			
		}
	}
	
}

func DelProfile(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	email := params.Get(":email")
	log.Println(`{"email": "` + email + `", "$dropall" : true}`)
	coll.Update(`{"email": "` + email + `", "$dropall" : true}`)
	
	w.WriteHeader(http.StatusNoContent)
	
	
	for _, each := range config.Replication.Replica {
		client, err := rpc.Dial("tcp", each)
		if err != nil {
			log.Fatal(err)
		}
		var flag bool
		err = client.Call("Listener.DeleteRep", []byte(email), &flag)
		if err != nil {
			log.Fatal(err)
		}
	}
}



func PutProfile(w http.ResponseWriter, r *http.Request) {
	
	params := r.URL.Query()
	email := params.Get(":email")
	
	query := `{"email": "` + email + `"}`
	log.Println(query)
	res, _ := coll.Find(query)
	log.Println(len(res))
	prof := &Request{}
	if len(res) > 0 {
		bson.Unmarshal(res[0], &prof)

		body, _ := ioutil.ReadAll(r.Body)
		Newprof := &Request{}
		err := json.Unmarshal([]byte(body), &Newprof)
		
		if err != nil {
			panic(err)
			w.Write([]byte(err.Error()))
			return
		}
		if len(Newprof.Email) != 0 {
			prof.Email = Newprof.Email
		}
		if len(Newprof.Zip) != 0 {
			prof.Zip = Newprof.Zip
		}
		if len(Newprof.Country) != 0 {
			prof.Country = Newprof.Country
		}
		if len(Newprof.Profession) != 0 {
			prof.Profession = Newprof.Profession
		}
		if len(Newprof.Favorite_color) != 0 {
			prof.Favorite_color = Newprof.Favorite_color
		}
		if len(Newprof.Is_smoking) != 0 {
			prof.Is_smoking = Newprof.Is_smoking
		}
		if len(Newprof.Favorite_sport) != 0 {
			prof.Favorite_sport = Newprof.Favorite_sport
		}
		if len(Newprof.Food.Type) != 0 {
			prof.Food.Type = Newprof.Food.Type
		}
		if len(Newprof.Food.Drink_alcohol) != 0 {
			prof.Food.Drink_alcohol = Newprof.Food.Drink_alcohol
		}
		if len(Newprof.Music.Spotify_user_id) != 0 {
			prof.Music.Spotify_user_id = Newprof.Music.Spotify_user_id
		}
		if len(Newprof.Movie.Tv_shows[0]) != 0 {
			prof.Movie.Tv_shows[0] = Newprof.Movie.Tv_shows[0]
		}
		if len(Newprof.Movie.Tv_shows[1]) != 0 {
			prof.Movie.Tv_shows[1] = Newprof.Movie.Tv_shows[1]
		}
		if len(Newprof.Movie.Tv_shows[2]) != 0 {
			prof.Movie.Tv_shows[2] = Newprof.Movie.Tv_shows[2]
		}
		if len(Newprof.Movie.Movies[0]) != 0 {
			prof.Movie.Movies[0] = Newprof.Movie.Movies[0]
		}
		if len(Newprof.Movie.Movies[1]) != 0 {
			prof.Movie.Movies[1] = Newprof.Movie.Movies[1]
		}
		if len(Newprof.Movie.Movies[2]) != 0 {
			prof.Movie.Movies[2] = Newprof.Movie.Movies[2]
		}
		if len(Newprof.Travel.Flight.Seat) != 0 {
			prof.Travel.Flight.Seat = Newprof.Travel.Flight.Seat
		}
	}
	updatedProfile, err := json.Marshal(prof)
	if err != nil {
		panic(err)
		w.Write([]byte(err.Error()))
	} else {
		var query = `{"email": "` + email + `", "$set":` + string(updatedProfile) + `}`
		log.Println(query)
		coll.Update(query)
		
	}
	
	w.WriteHeader(http.StatusNoContent)
	

	for _, each := range config.Replication.Replica {
		client, err := rpc.Dial("tcp", each)
		if err != nil {
			log.Fatal(err)
		}
		var flag bool
		err = client.Call("Listener.UpdateRep", updatedProfile, &flag)
		if err != nil {
			log.Fatal(err)
		}
	}
}



func main() {
	ConfigFile()

	jb, err := goejdb.Open(config.Database.File_name, goejdb.JBOWRITER|goejdb.JBOCREAT)
	if err != nil {
		os.Exit(1)
	}
	go RPCServer()

	
	coll, _ = jb.CreateColl("profile", nil)
	mux := routes.New()

	mux.Get("/profile/:email", GetProfile)
	mux.Post("/profile", PostProfile)
	mux.Put("/profile/:email", PutProfile)
	mux.Del("/profile/:email", DelProfile)
	http.Handle("/", mux)
	log.Println("Listening...")
	log.Println(config.Database.Port_num)
	http.ListenAndServe(":"+strconv.Itoa(config.Database.Port_num), nil)
	
	jb.Close()
}