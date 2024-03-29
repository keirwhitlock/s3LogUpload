package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/yaml.v2"
)

type debugLogger struct {
	enableDebug bool
}

func (d *debugLogger) Debug(args ...interface{}) {
	if d.enableDebug {
		log.Print(args...)
	}
}

var dbug = debugLogger{}

type Config struct {
	AWSBucket    string                       `yaml:"AWSBucket"`
	Env          string                       `yaml:"Env"`
	Region       string                       `yaml:"Region"`
	LogDirectory string                       `yaml:"LogDirectory"`
	Debug        bool                         `yaml:"Debug"`
	LogTypes     map[string]map[string]string `yaml:"LogTypes"`
}

func (c *Config) Parse(data []byte) error {
	if err := yaml.Unmarshal(data, c); err != nil {
		return err
	}
	return nil
}

var configFile *string

func readConfig() Config {
	_, err := os.Stat(*configFile)
	if err != nil {
		log.Fatal("Config file is missing: ", *configFile)
	}

	file, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	c := Config{}
	if err := c.Parse(file); err != nil {
		log.Fatal(err)
	}

	return c
}

func awsConnect(c *Config) (*session.Session, error) {
	if c.Env == "" {
		return session.NewSession(&aws.Config{
			Region: aws.String(c.Region),
		})
	}
	return session.NewSession(&aws.Config{
		Region:      aws.String(c.Region),
		Credentials: credentials.NewSharedCredentials("", c.Env),
	})
}

func s3Upload(bucket, key, myFile string, sess *session.Session) error {
	svc := s3manager.NewUploader(sess)

	f, err := os.Open(myFile)
	if err != nil {
		return err
	}
	defer f.Close()

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	fmt.Printf("file uploaded to, %s\n", result.Location)
	return err
}

func listFiles(logDir string) ([]string, error) {
	dbug.Debug("Filepath received: ", logDir)
	var files []string

	err := filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		path = strings.ReplaceAll(path, logDir, "")
		files = append(files, path)
		return nil
	})
	dbug.Debug(files)
	return files, err

}

func checkObjectExits(sess *session.Session, AWSBucket, key string, fileSize int64) bool {
	svc := s3.New(sess)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(AWSBucket),
		Key:    aws.String(key),
	}
	result, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				return false
			default:
				log.Fatal(err)
			}
		}
	}
	if *result.ContentLength != fileSize {
		return false
	}
	return true
}

func main() {

	configFile = flag.String("config", "config.yml", "Config file location")
	flag.Parse()

	config := readConfig()

	dbug.enableDebug = config.Debug

	sess, err := awsConnect(&config)
	if err != nil {
		log.Fatal(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	dtNow := time.Now().Format("2006-01-02")
	re1, err := regexp.Compile(dtNow)
	if err != nil {
		log.Fatal(err)
	}
	dateToday := strings.ReplaceAll(dtNow, "-", "/")

	files, err := listFiles(config.LogDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		matched := re1.MatchString(file)
		if matched {
			for _, a := range config.LogTypes {
				if strings.HasPrefix(file, a["LogPrefix"]) && strings.HasSuffix(file, ".csv") {
					key := fmt.Sprintf("%v/%v/%v/%v", a["DirectoryName"], dateToday, hostname, file)
					fileNamePath := fmt.Sprintf("%v%v", config.LogDirectory, file)

					fi, err := os.Stat(fileNamePath)
					if err != nil {
						log.Fatal(err)
					}
					if !checkObjectExits(sess, config.AWSBucket, key, fi.Size()) {
						err = s3Upload(config.AWSBucket, key, fileNamePath, sess)
						if err != nil {
							log.Fatal(err)
						}
					}
				}
			}
		}
	}

}
