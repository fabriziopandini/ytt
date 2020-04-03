package workspace

import (
	"fmt"
	"strings"

	"github.com/k14s/ytt/pkg/filepos"
	"github.com/k14s/ytt/pkg/files"
	"github.com/k14s/ytt/pkg/template"
	"github.com/k14s/ytt/pkg/template/core"
	"github.com/k14s/ytt/pkg/yamlmeta"
	"github.com/k14s/ytt/pkg/yamltemplate"
	"github.com/k14s/ytt/pkg/yttlibrary"
	yttoverlay "github.com/k14s/ytt/pkg/yttlibrary/overlay"
	"go.starlark.net/starlark"
)

type DataValuesPreProcessing struct {
	valuesFiles           []*FileInLibrary
	valuesOverlays        []*DataValues
	loader                *TemplateLoader
	IgnoreUnknownComments bool // TODO remove?
}

func (o DataValuesPreProcessing) Apply() (*DataValues, []*DataValues, error) {
	files := append([]*FileInLibrary{}, o.valuesFiles...)

	// Respect assigned file order for data values overlaying to succeed
	SortFilesInLibrary(files)

	dataDoc, libraryDataDocs, err := o.apply(files)
	if err != nil {
		errMsg := "Overlaying data values (in following order: %s): %s"
		return nil, nil, fmt.Errorf(errMsg, o.allFileDescs(files), err)
	}

	return dataDoc, libraryDataDocs, nil
}

func (o DataValuesPreProcessing) apply(files []*FileInLibrary) (*DataValues, []*DataValues, error) {
	var values *yamlmeta.Document
	var libraryValues []*DataValues
	for _, fileInLib := range files {
		valuesDocs, err := o.templateFile(fileInLib)
		if err != nil {
			return nil, nil, fmt.Errorf("Templating file '%s': %s", fileInLib.File.RelativePath(), err)
		}

		for _, valuesDoc := range valuesDocs {
			valDoc, err := NewDataValues(valuesDoc)
			if err != nil {
				return nil, nil, err
			}

			switch {
			case valDoc.HasLib():
				libraryValues = append(libraryValues, valDoc)
			case values == nil:
				values = valuesDoc
			default:
				var err error
				values, err = o.overlay(values, valDoc.Doc)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	values, err := o.overlayValuesOverlays(values)
	if err != nil {
		return nil, nil, err
	}

	valueDoc, err := NewDataValues(values)
	if err != nil {
		return nil, nil, err
	}

	return valueDoc, libraryValues, nil
}

func (p DataValuesPreProcessing) allFileDescs(files []*FileInLibrary) string {
	var result []string
	for _, fileInLib := range files {
		result = append(result, fileInLib.File.RelativePath())
	}
	if len(p.valuesOverlays) > 0 {
		result = append(result, "additional data values")
	}
	return strings.Join(result, ", ")
}

func (p DataValuesPreProcessing) templateFile(fileInLib *FileInLibrary) ([]*yamlmeta.Document, error) {
	libraryCtx := LibraryExecutionContext{Current: fileInLib.Library, Root: NewRootLibrary(nil)}

	_, resultDocSet, err := p.loader.EvalYAML(libraryCtx, fileInLib.File)
	if err != nil {
		return nil, err
	}

	tplOpts := yamltemplate.MetasOpts{IgnoreUnknown: p.IgnoreUnknownComments}

	// Extract _all_ data values docs from the templated result
	valuesDocs, nonValuesDocs, err := yttlibrary.DataValues{resultDocSet, tplOpts}.Extract()
	if err != nil {
		return nil, err
	}

	// Fail if there any non-empty docs that are not data values
	if len(nonValuesDocs) > 0 {
		for _, doc := range nonValuesDocs {
			if !doc.IsEmpty() {
				errStr := "Expected data values file '%s' to only have data values documents"
				return nil, fmt.Errorf(errStr, fileInLib.File.RelativePath())
			}
		}
	}

	return valuesDocs, nil
}

func (p DataValuesPreProcessing) overlay(valuesDoc, newValuesDoc *yamlmeta.Document) (*yamlmeta.Document, error) {
	op := yttoverlay.OverlayOp{
		Left:   &yamlmeta.DocumentSet{Items: []*yamlmeta.Document{valuesDoc}},
		Right:  &yamlmeta.DocumentSet{Items: []*yamlmeta.Document{newValuesDoc}},
		Thread: &starlark.Thread{Name: "data-values-pre-processing"},

		ExactMatch: true,
	}

	newLeft, err := op.Apply()
	if err != nil {
		return nil, err
	}

	return newLeft.(*yamlmeta.DocumentSet).Items[0], nil
}

func (p DataValuesPreProcessing) overlayValuesOverlays(valuesDoc *yamlmeta.Document) (*yamlmeta.Document, error) {
	if valuesDoc == nil {
		// TODO get rid of assumption that data values is a map?
		valuesDoc = &yamlmeta.Document{
			Value:    &yamlmeta.Map{},
			Position: filepos.NewUnknownPosition(),
		}
	}

	var result *yamlmeta.Document

	// by default return itself
	result = valuesDoc

	for _, valuesOverlay := range p.valuesOverlays {
		var err error

		result, err = p.overlay(result, valuesOverlay.Doc)
		if err != nil {
			// TODO improve error message?
			return nil, fmt.Errorf("Overlaying additional data values on top of "+
				"data values from files (marked as @data/values): %s", err)
		}
	}

	return result, nil
}

type DataValues struct {
	Doc         *yamlmeta.Document
	LibPath     []LibPathPiece
	AfterLibMod bool
}

type LibPathPiece struct {
	LibName string
	Tag     string
}

const (
	AnnotationLibraryName = "library/name"
)

func NewDataValues(doc *yamlmeta.Document) (*DataValues, error) {
	result := &DataValues{Doc: doc}

	anns := template.NewAnnotations(doc)
	if anns.Has(AnnotationLibraryName) {
		libArgs := template.NewAnnotations(doc).Args(AnnotationLibraryName)
		if l := libArgs.Len(); l != 1 {
			return nil, fmt.Errorf("%s annotation expects one arg, got %d", yttlibrary.AnnotationDataValues, l)
		}

		argString, err := core.NewStarlarkValue(libArgs[0]).AsString()
		if err != nil {
			return nil, err
		}

		err = result.SetLibAndTag(argString)
		if err != nil {
			return nil, fmt.Errorf("Annotation %s: %s", AnnotationLibraryName, err.Error())
		}
	}

	dvKwargs := template.NewAnnotations(doc).Kwargs(yttlibrary.AnnotationDataValues)
	for _, kwarg := range dvKwargs {
		kwargName, err := core.NewStarlarkValue(kwarg[0]).AsString()
		if err != nil {
			return nil, err
		}

		switch kwargName {
		case "after_library_module":
			afterLibMod, err := core.NewStarlarkValue(kwarg[1]).AsBool()
			if err != nil {
				return nil, err
			} else if !result.HasLib() {
				return nil, fmt.Errorf("%s annotation: cannot use kwarg 'after_library_module' without %s annotation", yttlibrary.AnnotationDataValues, AnnotationLibraryName)
			}
			result.AfterLibMod = afterLibMod
		default:
			return nil, fmt.Errorf("Unknown kwarg %s for annotation %s", kwargName, yttlibrary.AnnotationDataValues)
		}
	}

	return result, nil
}

func (dvd *DataValues) SetLibAndTag(libStr string) error {
	if libStr == "" {
		return fmt.Errorf("library name cannot be empty")
	}

	if !strings.HasPrefix(libStr, "@") {
		return fmt.Errorf("library string must being with '@'")
	}

	for _, libPathPiece := range strings.Split(libStr, "@")[1:] {
		libAndTag := strings.SplitN(libPathPiece, "~", 2)
		piece := LibPathPiece{LibName: libAndTag[0]}
		if len(libAndTag) == 2 {
			if libAndTag[1] == "" {
				return fmt.Errorf("library cannot have empty tag")
			}
			piece.Tag = libAndTag[1]
		}
		dvd.LibPath = append(dvd.LibPath, piece)
	}

	return nil
}

func (dvd *DataValues) HasLib() bool { return dvd.LibPath != nil }

func (dvd *DataValues) PopLib() (string, string, bool) {
	if len(dvd.LibPath) == 0 {
		panic("DataValue was not used by specified library")
	}

	_, name := files.SplitPath(dvd.LibPath[0].LibName)
	tag := dvd.LibPath[0].Tag
	final := len(dvd.LibPath) == 1
	dvd.LibPath = dvd.LibPath[1:]
	return name, tag, final
}
