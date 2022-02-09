package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {

	f := flag.NewFlagSet("buspass", flag.ExitOnError)
	f.SetOutput(os.Stdout)

	// variables declaration
	var name string
	var namespace string
	var bodyFile string
	var priority int
	var repeat int

	// flags declaration using flag package
	f.StringVar(&name, "n", "", "Specify queue/topic name. ")
	f.StringVar(&namespace, "ns", "", "Specify servicebus namespace name (e.g myns)")
	f.StringVar(&bodyFile, "f", "", "Specify file for message body")
	f.IntVar(&priority, "p", 0, "Specify priority, default is no priority")
	f.IntVar(&repeat, "rep", 1, "Specify the number of times to send message, default is 1")

	err := f.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}

	if name == "" {
		println("queue/topic name is required")
		os.Exit(1)
	}
	if namespace == "" {
		println("service bus namespace is required")
		os.Exit(1)
	}
	if bodyFile == "" {
		println("path to message body file is required")
		os.Exit(1)
	}

	data, err := os.ReadFile(bodyFile)
	if err != nil {
		panic(err)
	}

	credential, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{ClientOptions: policy.ClientOptions{Logging: policy.LogOptions{IncludeBody: true}}})

	if err != nil {
		panic(err)
	}

	// The service principal specified by the credential needs to be added to the appropriate Service Bus roles for your
	// resource. More information about Service Bus roles can be found here:
	// https://docs.microsoft.com/azure/service-bus-messaging/service-bus-managed-service-identity#azure-built-in-roles-for-azure-service-bus
	client, err := azservicebus.NewClient(fmt.Sprintf("%s.servicebus.windows.net", namespace), credential, nil)

	if err != nil {
		panic(err)
	}

	defer client.Close(context.TODO())

	sender, err := client.NewSender(name, &azservicebus.NewSenderOptions{})

	if err != nil {
		panic(err)
	}

	defer sender.Close(context.TODO())

	contentType := "application/json"

	properties := map[string]interface{}{
		"Priority": fmt.Sprintf("P%d", priority),
	}

	if priority == 0 {
		properties = map[string]interface{}{}
	}

	for n := 0; n < repeat; n++ {

		err = sender.SendMessage(context.TODO(), &azservicebus.Message{Body: data, ContentType: &contentType, ApplicationProperties: properties})
		if err != nil {
			panic(err)
		}

	}

	println("finished send to", name, ",", repeat, "times")
}
