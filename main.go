package main

import (
	"fmt"
	"time"
)

type WorkerPools struct {
	jobMap            map[string]struct{}
	Jobs              int
	InfoJobs          chan string
	FolderJobs        chan string
	AudioJobs         chan string
	VideoJobs         chan string
	SubtitleJobs      chan string
	ObjectStorageJobs chan string
}

type jobs struct {
	Name  string
	Stage string
}

var (
	jobsArray = []jobs{
		{
			Name:  "1",
			Stage: "new",
		},
		{
			Name:  "2",
			Stage: "audio",
		},
		{
			Name:  "3",
			Stage: "folder",
		},
		{
			Name:  "4",
			Stage: "video",
		},
		{
			Name:  "5",
			Stage: "subtitle",
		},
		{
			Name:  "6",
			Stage: "object-storage",
		},
	}

	jobsCount = 6

	workerPool = WorkerPools{
		Jobs:              jobsCount,
		InfoJobs:          make(chan string, jobsCount),
		FolderJobs:        make(chan string, jobsCount),
		AudioJobs:         make(chan string, jobsCount),
		VideoJobs:         make(chan string, jobsCount),
		SubtitleJobs:      make(chan string, jobsCount),
		ObjectStorageJobs: make(chan string, jobsCount),
	}

	stagesMatrix = map[string]string{
		"new":      "audio",
		"audio":    "video",
		"video":    "subtitle",
		"subtitle": "object-storage",
	}
)

func (w *WorkerPools) getInfo() {
	for job := range w.InfoJobs {
		fmt.Printf("retrieving job %s from database....\n", job)
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("retrieving info for %s...\n", job)
		time.Sleep(time.Millisecond * 400)
		fmt.Printf("updating file info for %s...\n", job)

		fmt.Println("info finished...")

		delete(w.jobMap, job)
		fmt.Println("PAIR", stagesMatrix["new"])
		w.distributeJobs([]jobs{
			{
				Name:  job,
				Stage: stagesMatrix["new"],
			},
		})
	}
}

func (w *WorkerPools) getAudio() {
	for job := range w.AudioJobs {
		fmt.Printf("retrieving job %s from database....\n", job)
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("extracting audio for %s...\n", job)
		time.Sleep(time.Millisecond * 400)
		fmt.Printf("updating file info for %s...\n", job)

		fmt.Println("audio finished...")

		delete(w.jobMap, job)

		w.distributeJobs([]jobs{
			{
				Name:  job,
				Stage: stagesMatrix["audio"],
			},
		})
	}
}

func (w *WorkerPools) getVideo() {
	for job := range w.VideoJobs {
		fmt.Printf("retrieving job %s from database....\n", job)
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("extracting video for %s...\n", job)
		time.Sleep(time.Millisecond * 400)
		fmt.Printf("updating file info for %s...\n", job)

		fmt.Println("video finished...")

		delete(w.jobMap, job)

		w.distributeJobs([]jobs{
			{
				Name:  job,
				Stage: stagesMatrix["video"],
			},
		})
	}
}

func (w *WorkerPools) getSubtitle() {
	for job := range w.SubtitleJobs {
		fmt.Printf("retrieving job %s from database....\n", job)
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("extracting subtitle for %s...\n", job)
		time.Sleep(time.Millisecond * 400)
		fmt.Printf("updating file info for %s...\n", job)

		fmt.Println("subtitle finished...")

		delete(w.jobMap, job)

		w.distributeJobs([]jobs{
			{
				Name:  job,
				Stage: stagesMatrix["subtitle"],
			},
		})
	}
}

func (w *WorkerPools) objectStorage() {
	for job := range w.ObjectStorageJobs {
		fmt.Printf("retrieving job %s from database....\n", job)
		time.Sleep(time.Millisecond * 500)
		fmt.Printf("uploading file for %s...\n", job)
		time.Sleep(time.Millisecond * 400)
		fmt.Printf("updating file info for %s...\n", job)

		fmt.Println("uploading finished...")

		delete(w.jobMap, job)

		w.distributeJobs([]jobs{
			{
				Name:  job,
				Stage: stagesMatrix["object-storage"],
			},
		})
	}
}

func (w *WorkerPools) distributeJobs(tasks []jobs) int {
	var (
		infoStage          = "new"
		audioStage         = "audio"
		folderStage        = "folder"
		videoStage         = "video"
		subtitleStage      = "subtitle"
		objectStorageStage = "object-storage"
		existsCount        = 0
	)

	for _, task := range tasks {
		if _, exists := w.jobMap[task.Name]; exists {
			fmt.Println("already exists in queue")
			existsCount++
		} else {
			fmt.Println("new job arrived...")
			w.jobMap[task.Name] = struct{}{}
		}

		switch task.Stage {
		case infoStage:
			fmt.Println("INFO STAGE: ", task.Name)
			w.InfoJobs <- task.Name
		case audioStage:
			fmt.Println("AUDIO STAGE: ", task.Name)
			w.AudioJobs <- task.Name
		case folderStage:
			fmt.Println("FOLDER STAGE: ", task.Name)
			w.FolderJobs <- task.Name
		case videoStage:
			fmt.Println("VIDEO STAGE: ", task.Name)
			w.VideoJobs <- task.Name
		case subtitleStage:
			fmt.Println("SUBTITLE STAGE: ", task.Name)
			w.SubtitleJobs <- task.Name
		case objectStorageStage:
			fmt.Println("OBJECT STORAGE STAGE: ", task.Name)
			w.ObjectStorageJobs <- task.Name
		default:
			fmt.Println("NOT FOUND STAGE exiting...")
		}
	}

	return existsCount
}

func main() {
	fmt.Println(workerPool)
	workerPool.jobMap = make(map[string]struct{})

	go workerPool.getInfo()
	go workerPool.getAudio()
	go workerPool.getVideo()
	go workerPool.getSubtitle()
	go workerPool.objectStorage()

	workerPool.distributeJobs(jobsArray)

	time.Sleep(10 * time.Second)
}
