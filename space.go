package nebulagolang

import (
	"errors"
	"fmt"
	"github.com/samber/lo"
	"github.com/thalesfu/nebulagolang/utils"
	"strings"
)

type Space struct {
	Name   string    `yaml:"name"`
	Nebula *NebulaDB `yaml:"nebula"`
}

func (s *Space) Execute(stmts ...string) *Result {
	finalStmts := []string{s.UseCommand()}
	finalStmts = append(finalStmts, stmts...)

	resultSet, ok, err := s.Nebula.Execute(finalStmts...)

	return NewResult(resultSet, ok, err, finalStmts...)
}

func (s *Space) Drop() *Result {
	fmt.Println(utils.PrintColorRed + "大警告! 你将删除" + s.Name + "这个空间. WARNING! You are going to drop the space " + s.Name + "!" + utils.PrintColorReset)
	fmt.Println(utils.PrintColorRed + "如果你真的要删除，请输入\"我真的要删除" + s.Name + "这个空间\"" + utils.PrintColorReset)
	var input string
	fmt.Scanln(&input)
	if input == "我真的要删除"+s.Name+"这个空间" {
		fmt.Println("就是不给你删，气死你！")
		return NewErrorResult(errors.New("就是不给你删，气死你！"))
	} else if input == "气死了" {
		fmt.Println("好吧，那就删了吧！")
	} else {
		fmt.Println("不给你删！")
		return NewErrorResult(errors.New("不给你删！"))
	}

	stmt := "DROP SPACE IF EXISTS " + s.Name
	return s.Execute(stmt)
}

func (s *Space) Describe() *Result {
	stmt := "Describe space " + s.Name
	return s.Execute(stmt)
}

func (s *Space) UseCommand() string {
	return "USE " + s.Name
}

func (s *Space) ShowTags() *Result {
	command := []string{
		"SHOW TAGS",
	}

	return s.Execute(command...)
}

func (s *Space) CreateTag(tag *TagSchema) *Result {
	return s.Execute(tag.CreateString())
}

func (s *Space) CreateTagWithIndexes(tag *TagSchema) *Result {
	cmds := make([]string, 0)

	r := s.CreateTag(tag)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	for _, idx := range tag.Indexes {
		r = s.CreateTagIndex(idx)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func (s *Space) DropTag(tag string) *Result {
	command := []string{
		"DROP TAG IF EXISTS " + tag,
	}

	return s.Execute(command...)
}

func (s *Space) DropTagWithIndexes(tag string) *Result {
	cmds := make([]string, 0)

	r := s.DropTagIndexByTagName(tag)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	r = s.DropTag(tag)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	return NewSuccessResult(cmds...)
}

func (s *Space) RebuildTagWithIndexes(tag *TagSchema) *Result {
	r := s.DropTagWithIndexes(tag.Name)

	if !r.Ok {
		return r
	}

	return s.CreateTagWithIndexes(tag)
}

func (s *Space) AddTagProperty(tag string, property *TagPropertySchema) *Result {
	command := []string{
		"ALTER TAG " + tag + " ADD (" + property.String() + ")",
	}

	return s.Execute(command...)
}

func (s *Space) AddTagProperties(tag string, properties []*TagPropertySchema) *Result {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"ALTER TAG " + tag + " ADD (" + strings.Join(propertiesString, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) ChangeTagProperty(tag string, property *TagPropertySchema) *Result {
	command := []string{
		"ALTER TAG " + tag + " CHANGE (" + property.String() + ")",
	}

	return s.Execute(command...)
}

func (s *Space) ChangeTagProperties(tag string, properties []*TagPropertySchema) *Result {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"ALTER TAG " + tag + " CHANGE (" + strings.Join(propertiesString, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) DropTagProperty(tag string, property string) *Result {
	command := []string{
		"ALTER TAG " + tag + " DROP (" + property + ")",
	}

	return s.Execute(command...)
}

func (s *Space) DropTagProperties(tag string, properties []string) *Result {
	command := []string{
		"ALTER TAG " + tag + " DROP (" + strings.Join(properties, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) DescribeTag(tag string) *Result {
	command := []string{
		"DESCRIBE TAG " + tag,
	}

	return s.Execute(command...)
}

func (s *Space) ShowTagIndexes() *Result {
	command := []string{
		"SHOW TAG INDEXES",
	}

	return s.Execute(command...)
}

func (s *Space) ShowTagIndexesByTagName(tagName string) *ResultT[[]string] {
	r := s.ShowTagIndexes()

	if !r.Ok {
		return NewResultT[[]string](r)
	}

	result := make([]string, 0)

	idxNames, err := r.DataSet.GetValuesByColName("Index Name")

	if err != nil {
		return NewErrorResultT[[]string](err)
	}

	for _, idxName := range idxNames {
		idxn, err := idxName.AsString()
		if err != nil {
			return NewErrorResultT[[]string](err)
		}

		if strings.HasPrefix(idxn, getTagIndexPrefix(tagName)) {
			result = append(result, idxn)
		}
	}

	return NewResultTWithData(r, result)
}

func (s *Space) CreateTagIndex(tagIndex *TagIndexSchema) *Result {
	return s.Execute(tagIndex.CreateIndexString())
}

func (s *Space) DropTagIndex(indexName ...string) *Result {
	commands := make([]string, len(indexName))
	for i, idx := range indexName {
		commands[i] = "DROP TAG INDEX IF EXISTS " + idx
	}

	return s.Execute(commands...)
}

func (s *Space) DropTagIndexByTagName(tagName string) *Result {
	r := s.ShowTagIndexesByTagName(tagName)
	if !r.Ok {
		return r.Result
	}

	return s.DropTagIndex(r.Data...)
}

func (s *Space) DescribeTagIndex(indexName string) *Result {
	command := []string{
		"DESCRIBE TAG INDEX " + indexName,
	}

	return s.Execute(command...)
}

func (s *Space) RebuildTagIndex(indexName string) *Result {
	command := []string{
		"REBUILD TAG INDEX " + indexName,
	}

	return s.Execute(command...)
}

func (s *Space) ShowTagIndexStatus() *Result {
	command := []string{
		"SHOW TAG INDEX STATUS",
	}

	return s.Execute(command...)
}

func (s *Space) BatchInsertMultiTagVertexes(batch int, vs []MultiTagEntity) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	chunk := lo.Chunk(vs, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := s.InsertMultiTagVertexes(c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			return NewErrorResult(errors.New(fmt.Sprintf("insert batch %d multitag vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error())))
		}
	}

	return NewSuccessResult(cmds...)
}

func (s *Space) InsertMultiTagVertexes(vs ...MultiTagEntity) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	vst, vsv := make([]string, len(vs)), make([]string, len(vs))

	for i, v := range vs {

		tags := v.GetTags()
		tagsWithProperties := make([]string, 0)
		tagsPropertyValueList := make([]string, 0)

		for _, tag := range tags {
			tagWithProperties, propertyValueList := GetAllInsertTagWithPropertiesAndPropertyValueList(tag)
			tagsWithProperties = append(tagsWithProperties, tagWithProperties)
			tagsPropertyValueList = append(tagsPropertyValueList, propertyValueList...)
		}

		vst[i] = strings.Join(tagsWithProperties, ", ")
		vsv[i] = "\"" + v.VID() + "\":(" + strings.Join(tagsPropertyValueList, ", ") + ")"
	}

	command := []string{
		"INSERT VERTEX IF NOT EXISTS " + vst[0] + " VALUES " + strings.Join(vsv, ", ") + ";",
	}

	return s.Execute(command...)
}

func (s *Space) ShowEdges() *Result {
	command := []string{
		"SHOW EDGES",
	}

	return s.Execute(command...)
}

func (s *Space) CreateEdge(edge *EdgeSchema) *Result {
	command := []string{
		edge.CreateString(),
	}

	return s.Execute(command...)
}

func (s *Space) DropEdge(edgeName string) *Result {
	command := []string{
		"DROP EDGE IF EXISTS " + edgeName,
	}

	return s.Execute(command...)
}

func (s *Space) DescribeEdge(edge string) *Result {
	command := []string{
		"DESCRIBE EDGE " + edge,
	}

	return s.Execute(command...)
}

func (s *Space) AddEdgeProperty(edge string, property *EdgePropertySchema) *Result {
	command := []string{
		"ALTER EDGE " + edge + " ADD (" + property.String() + ")",
	}

	return s.Execute(command...)
}

func (s *Space) AddEdgeProperties(edge string, properties []*EdgePropertySchema) *Result {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"ALTER EDGE " + edge + " ADD (" + strings.Join(propertiesString, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) ChangeEdgeProperty(edge string, property *EdgePropertySchema) *Result {
	command := []string{
		"ALTER EDGE " + edge + " CHANGE (" + property.String() + ")",
	}

	return s.Execute(command...)
}

func (s *Space) ChangeEdgeProperties(edge string, properties []*EdgePropertySchema) *Result {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"ALTER EDGE " + edge + " CHANGE (" + strings.Join(propertiesString, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) DropEdgeProperty(edge string, property string) *Result {
	command := []string{
		"ALTER EDGE " + edge + " DROP (" + property + ")",
	}

	return s.Execute(command...)
}

func (s *Space) DropEdgeProperties(edge string, properties []string) *Result {
	command := []string{
		"ALTER EDGE " + edge + " DROP (" + strings.Join(properties, ", ") + ")",
	}

	return s.Execute(command...)
}

func (s *Space) ShowEdgeIndexes() *Result {
	command := []string{
		"SHOW EDGE INDEXES",
	}

	return s.Execute(command...)
}

func (s *Space) ShowEdgeIndexesByEdgeName(edgeName string) *ResultT[[]string] {
	r := s.ShowEdgeIndexes()

	if !r.Ok {
		return NewResultT[[]string](r)
	}

	result := make([]string, 0)

	idxNames, err := r.DataSet.GetValuesByColName("Index Name")

	if err != nil {
		return NewErrorResultT[[]string](err)
	}

	for _, idxName := range idxNames {
		idxn, err := idxName.AsString()
		if err != nil {
			return NewErrorResultT[[]string](err)
		}

		if strings.HasPrefix(idxn, getEdgeIndexPrefix(edgeName)) {
			result = append(result, idxn)
		}
	}

	return NewResultTWithData[[]string](r, result)
}

func (s *Space) CreateEdgeIndex(edgeIndex *EdgeIndexSchema) *Result {
	command := []string{
		edgeIndex.CreateIndexString(),
	}

	return s.Execute(command...)
}

func (s *Space) DropEdgeIndex(indexName ...string) *Result {
	command := make([]string, len(indexName))
	for i, idx := range indexName {
		command[i] = "DROP EDGE INDEX IF EXISTS " + idx
	}

	return s.Execute(command...)
}

func (s *Space) DropEdgeIndexByEdgeName(edgeName string) *Result {
	r := s.ShowEdgeIndexesByEdgeName(edgeName)
	if !r.Ok {
		return r.Result
	}

	return s.DropEdgeIndex(r.Data...)
}

func (s *Space) DescribeEdgeIndex(indexName string) *Result {
	command := []string{
		"DESCRIBE EDGE INDEX " + indexName,
	}

	return s.Execute(command...)
}

func (s *Space) RebuildEdgeIndex(indexName string) *Result {
	command := []string{
		"REBUILD EDGE INDEX " + indexName,
	}

	return s.Execute(command...)
}

func (s *Space) ShowEdgeIndexStatus() *Result {
	command := []string{
		"SHOW EDGE INDEX STATUS",
	}

	return s.Execute(command...)
}

func (s *Space) CreateEdgeWithIndexes(edge *EdgeSchema) *Result {
	cmds := make([]string, 0)
	r := s.CreateEdge(edge)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	for _, idx := range edge.Indexes {
		r = s.CreateEdgeIndex(idx)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func (s *Space) DropEdgeWithIndexes(edge string) *Result {
	cmds := make([]string, 0)
	r := s.DropEdgeIndexByEdgeName(edge)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	r = s.DropEdge(edge)
	cmds = append(cmds, r.Commands...)

	if !r.Ok {
		return r
	}

	return NewSuccessResult(cmds...)
}

func (s *Space) RebuildEdgeWithIndexes(edge *EdgeSchema) *Result {
	r := s.DropEdgeWithIndexes(edge.Name)

	if !r.Ok {
		return r
	}

	return s.CreateEdgeWithIndexes(edge)
}
