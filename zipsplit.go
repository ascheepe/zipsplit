package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"errors"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type Config struct {
	sourceArchive string
	nameTemplate  string
	splitSize     uint64
}

type Bucket struct {
	config   Config
	filename string
	size     uint64
	files    []*zip.FileHeader
}

type bySize []*zip.FileHeader

func (a bySize) Len() int {
	return len(a)
}

func (a bySize) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a bySize) Less(i, j int) bool {
	return a[i].CompressedSize64 < a[j].CompressedSize64
}

// Return a function which increases the number used
// for the format string each time it is called.
func numberedFileNamer(template string) (func() string, error) {
	// The provided template should change when provided
	// with different numbers but not contain the error
	// format string.
	a := fmt.Sprintf(template, 0)
	b := fmt.Sprintf(template, 1)
	if a == b || strings.Contains(a, "%!") {
		return nil, errors.New("Invalid template.")
	}

	n := 1
	return func() string {
		name := fmt.Sprintf(template, n)
		n = n + 1
		return name
	}, nil
}

func getZipContents(zipfile string) ([]*zip.FileHeader, error) {
	var files []*zip.FileHeader

	r, err := zip.OpenReader(zipfile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for _, f := range r.File {
		files = append(files, &f.FileHeader)
	}

	return files, nil
}

func (bucket *Bucket) makeZip(config Config) error {
	sourceReader, err := zip.OpenReader(config.sourceArchive)
	if err != nil {
		return err
	}
	defer sourceReader.Close()

	zipDestination, err := os.Create(bucket.filename)
	if err != nil {
		return err
	}
	defer zipDestination.Close()

	w := zip.NewWriter(zipDestination)
	defer w.Close()

	for _, bucketFile := range bucket.files {
		for _, sourceFile := range sourceReader.File {
			if bucketFile.Name == sourceFile.Name {
				err := w.Copy(sourceFile)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	return nil
}

func fit(files []*zip.FileHeader, config Config) ([]*Bucket, error) {
	var buckets []*Bucket

	newZipName, err := numberedFileNamer(config.nameTemplate)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		added := false

		for _, bucket := range buckets {
			// Account for the overhead a zipfile has;
			// Magic numbers are header sizes.
			// Name is counted twice:
			// once for the local header and once for the central
			// directory at the end of the zipfile.
			totalSize := uint64(len(file.Name)) +
				30 + 16 + 46 +
				uint64(len(file.Name)) +
				uint64(len(file.Comment)) +
				uint64(len(file.Extra))

			totalSize += file.CompressedSize64
			if bucket.size+totalSize <= config.splitSize-22 {
				bucket.size += totalSize
				bucket.files = append(bucket.files, file)
				added = true
				break
			}
		}

		if !added {
			buckets = append(buckets, &Bucket{
				filename: newZipName(),
				size:     file.CompressedSize64,
				files:    []*zip.FileHeader{file}})
		}
	}

	return buckets, nil
}

// byte sizes
const (
	Byte = 1 << (iota * 10)
	KByte
	MByte
	GByte
	TByte
	EByte
)

var sizeTable = map[string]uint64{
	"":   Byte,
	"b":  Byte,
	"k":  KByte,
	"kb": KByte,
	"m":  MByte,
	"mb": MByte,
	"g":  GByte,
	"gb": GByte,
	"t":  TByte,
	"tb": TByte,
	"e":  EByte,
	"eb": EByte,
}

func numberToHuman(n uint64) string {
	units := []string{"b", "Kb", "Mb", "Gb", "Tb", "Eb"}
	value := float64(n)

	i := 0
	for ; value > KByte && i < len(units); i++ {
		value /= float64(KByte)
	}

	return fmt.Sprintf("%.2f%s", value, units[i])
}

func humanToNumber(s string) uint64 {
	var splitPoint uint64

	splitPoint = 0
	for _, r := range s {
		if !unicode.IsDigit(r) {
			break
		}
		splitPoint++
	}
	numberString := s[:splitPoint]
	number, err := strconv.ParseUint(numberString, 10, 64)
	if err != nil {
		return 0
	}

	suffix := strings.ToLower(strings.TrimSpace(s[splitPoint:]))
	factor := sizeTable[suffix]
	number *= factor

	return number
}

func main() {
	sourceArchive := flag.String(
		"in",
		"",
		"Input archive name.")

	splitSizeString := flag.String(
		"s",
		"10Mb",
		"Maximum size per part.")

	nameTemplate := flag.String(
		"out",
		"out-%03d.zip",
		"Output name template in printf format.")
	flag.Parse()

	if *sourceArchive == "" {
		err := fmt.Errorf("Please supply an input archive.")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	config := Config{
		sourceArchive: *sourceArchive,
		nameTemplate: *nameTemplate,
		splitSize: humanToNumber(*splitSizeString)}

	files, err := getZipContents(config.sourceArchive)
	if err != nil {
		log.Fatal(err)
	}
	sort.Sort(sort.Reverse(bySize(files)))

	if len(files) < 1 || files[0].CompressedSize64 > config.splitSize {
		fmt.Printf(
			"Can never fit %s (%s).\n",
			files[0].Name,
			numberToHuman(files[0].CompressedSize64))
		os.Exit(1)
	}

	buckets, err := fit(files, config)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	for _, bucket := range buckets {
		err := bucket.makeZip(config)
		if err != nil {
			log.Fatal(err)
		}
	}
}
