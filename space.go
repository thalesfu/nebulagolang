package nebulagolang

import (
	"errors"
	"fmt"
	"github.com/samber/lo"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	"strings"
)

type Space struct {
	Name   string    `yaml:"name"`
	Nebula *NebulaDB `yaml:"nebula"`
}

func (ns *Space) Execute(stmt string) (*nebulago.ResultSet, bool, error) {
	return ns.Nebula.Execute(stmt)
}

func (ns *Space) Drop() (*nebulago.ResultSet, bool, error) {
	red := "\033[31m"
	reset := "\033[0m"

	fmt.Println(red + "大警告! 你将删除" + ns.Name + "这个空间. WARNING! You are going to drop the space " + ns.Name + "!" + reset)
	fmt.Println(red + "如果你真的要删除，请输入\"我真的要删除" + ns.Name + "这个空间\"" + reset)
	var input string
	fmt.Scanln(&input)
	if input == "我真的要删除"+ns.Name+"这个空间" {
		fmt.Println("就是不给你删，气死你！")
		return nil, false, errors.New("就是不给你删，气死你！")
	} else if input == "气死了" {
		fmt.Println("好吧，那就删了吧！")
	} else {
		fmt.Println("不给你删！")
		return nil, false, errors.New("不给你删！")
	}

	stmt := "DROP SPACE IF EXISTS " + ns.Name + ";"
	return ns.Execute(stmt)
}

func (ns *Space) Describe() (*nebulago.ResultSet, bool, error) {
	stmt := "Describe space " + ns.Name + ";"
	return ns.Execute(stmt)
}

func (ns *Space) ShowTags() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW TAGS;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) CreateTag(tag *TagSchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		tag.CreateString(),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) CreateTagWithIndexes(tag *TagSchema) (bool, error) {
	_, ok, err := ns.CreateTag(tag)

	if !ok {
		return false, err
	}

	for _, idx := range tag.Indexes {
		_, ok, err = ns.CreateTagIndex(idx)

		if !ok {
			return false, err
		}
	}

	return true, nil
}

func (ns *Space) DropTag(tag string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DROP TAG IF EXISTS " + tag + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropTagWithIndexes(tag string) (bool, error) {

	_, ok, err := ns.DropTagIndexByTagName(tag)

	if !ok {
		return ok, err
	}

	_, ok, err = ns.DropTag(tag)

	if !ok {
		return ok, err
	}

	return true, nil
}

func (ns *Space) RebuildTagWithIndexes(tag *TagSchema) (bool, error) {
	ok, err := ns.DropTagWithIndexes(tag.Name)

	if !ok {
		return ok, err
	}

	return ns.CreateTagWithIndexes(tag)
}

func (ns *Space) AddTagProperty(tag string, property *TagPropertySchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " ADD (" + property.String() + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) AddTagProperties(tag string, properties []*TagPropertySchema) (*nebulago.ResultSet, bool, error) {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " ADD (" + strings.Join(propertiesString, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ChangeTagProperty(tag string, property *TagPropertySchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " CHANGE (" + property.String() + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ChangeTagProperties(tag string, properties []*TagPropertySchema) (*nebulago.ResultSet, bool, error) {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " CHANGE (" + strings.Join(propertiesString, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropTagProperty(tag string, property string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " DROP (" + property + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropTagProperties(tag string, properties []string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER TAG " + tag + " DROP (" + strings.Join(properties, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DescribeTag(tag string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DESCRIBE TAG " + tag + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowTagIndexes() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW TAG INDEXES;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowTagIndexesByTagName(tagName string) ([]string, bool, error) {
	resultSet, ok, err := ns.ShowTagIndexes()

	if !ok {
		return nil, false, err
	}

	result := make([]string, 0)

	idxNames, err := resultSet.GetValuesByColName("Index Name")

	if err != nil {
		return nil, false, err
	}

	for _, idxName := range idxNames {
		idxn, err := idxName.AsString()
		if err != nil {
			return nil, false, err
		}

		if strings.HasPrefix(idxn, getTagIndexPrefix(tagName)) {
			result = append(result, idxn)
		}
	}

	return result, true, nil
}

func (ns *Space) CreateTagIndex(tagIndex *TagIndexSchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		tagIndex.CreateIndexString(),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropTagIndex(indexName ...string) (*nebulago.ResultSet, bool, error) {
	command := make([]string, len(indexName)+1)
	command[0] = "USE " + ns.Name + ";"
	for i, idx := range indexName {
		command[i+1] = "DROP TAG INDEX IF EXISTS " + idx + ";"
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropTagIndexByTagName(tagName string) (*nebulago.ResultSet, bool, error) {
	idxNames, ok, err := ns.ShowTagIndexesByTagName(tagName)
	if !ok {
		return nil, false, err
	}

	return ns.DropTagIndex(idxNames...)
}

func (ns *Space) DescribeTagIndex(indexName string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DESCRIBE TAG INDEX " + indexName + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) RebuildTagIndex(indexName string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"REBUILD TAG INDEX " + indexName + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowTagIndexStatus() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW TAG INDEX STATUS;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) InsertVertex(t TagEntity) (*nebulago.ResultSet, bool, error) {
	pns, pvs := GetInsertPropertiesNamesAndValuesString(t)
	command := []string{
		"USE " + ns.Name + ";",
		"INSERT VERTEX IF NOT EXISTS " + t.GetTagName() + "(" + pns + ") VALUES \"" + t.VID() + "\":(" + pvs + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) InsertVertexes(vs ...TagEntity) (*nebulago.ResultSet, bool, error) {
	if len(vs) == 0 {
		return nil, false, errors.New("no vertexes")
	}

	pns, pvs := make([]string, len(vs)), make([]string, len(vs))

	for i, t := range vs {
		pn, pv := GetAllInsertPropertiesNamesAndValuesString(t)
		pns[i] = pn
		pvs[i] = "\"" + t.VID() + "\":(" + pv + ")"
	}

	command := []string{
		"USE " + ns.Name + ";",
		"INSERT VERTEX IF NOT EXISTS " + vs[0].GetTagName() + "(" + pns[0] + ") VALUES " + strings.Join(pvs, ", ") + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) BatchInsertVertexes(batch int, vs []TagEntity) (bool, error) {
	if len(vs) == 0 {
		return false, errors.New("no vertexes")
	}

	chunk := lo.Chunk(vs, batch)

	for i, c := range chunk {
		_, ok, err := ns.InsertVertexes(c...)

		if !ok {
			return false, errors.New(fmt.Sprintf("insert batch %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
		}
	}

	return true, nil
}

func (ns *Space) InsertMultiTagVertex(v MultiTagEntity) (*nebulago.ResultSet, bool, error) {
	tagsWithPropertiesString := ""
	tagsPropertyValueListString := ""

	tags := v.GetTags()
	tagsWithProperties := make([]string, len(tags))
	tagsPropertyValueList := make([]string, 0)

	for _, tag := range tags {
		tagWithProperties, propertyValueList := GetAllInsertTagWithPropertiesAndPropertyValueList(tag)
		tagsWithProperties = append(tagsWithProperties, tagWithProperties)
		tagsPropertyValueList = append(tagsPropertyValueList, propertyValueList...)
	}

	tagsWithPropertiesString = strings.Join(tagsWithProperties, ", ")
	tagsPropertyValueListString = "\"" + v.VID() + "\":(" + strings.Join(tagsPropertyValueList, ", ") + ")"

	command := []string{
		"USE " + ns.Name + ";",
		"INSERT VERTEX IF NOT EXISTS " + tagsWithPropertiesString + " VALUES " + tagsPropertyValueListString + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) BatchInsertMultiTagVertexes(batch int, vs []MultiTagEntity) (bool, error) {
	if len(vs) == 0 {
		return false, errors.New("no vertexes")
	}

	chunk := lo.Chunk(vs, batch)

	for i, c := range chunk {
		_, ok, err := ns.InsertMultiTagVertexes(c...)

		if !ok {
			return false, errors.New(fmt.Sprintf("insert batch %d multitag vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
		}
	}

	return true, nil
}

func (ns *Space) InsertMultiTagVertexes(vs ...MultiTagEntity) (*nebulago.ResultSet, bool, error) {
	if len(vs) == 0 {
		return nil, false, errors.New("no vertexes")
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
		"USE " + ns.Name + ";",
		"INSERT VERTEX IF NOT EXISTS " + vst[0] + " VALUES " + strings.Join(vsv, ", ") + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) UpdateVertex(v TagEntity) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		GetTagUpdateString(v),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) UpdateVertexes(vs ...TagEntity) (*nebulago.ResultSet, bool, error) {
	if len(vs) == 0 {
		return nil, false, errors.New("no vertexes")
	}

	command := make([]string, len(vs)+1)
	command[0] = "USE " + ns.Name + ";"
	for i, t := range vs {
		command[i+1] = GetTagUpdateString(t)
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) BatchUpdateVertexes(batch int, vs []TagEntity) (bool, error) {
	if len(vs) == 0 {
		return false, errors.New("no vertexes")
	}

	chunk := lo.Chunk(vs, batch)

	for i, c := range chunk {
		_, ok, err := ns.UpdateVertexes(c...)

		if !ok {
			return false, errors.New(fmt.Sprintf("update batch %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
		}
	}

	return true, nil
}

func (ns *Space) UpsertVertex(t TagEntity) (*nebulago.ResultSet, bool, error) {
	pns, pvs := GetUpdatePropertiesNamesAndValuesString(t)

	command := []string{
		"USE " + ns.Name + ";",
		"UPSERT VERTEX ON " + t.GetTagName() + " \"" + t.VID() + "\" SET " + pvs + " YIELD " + pns + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteVertex(t TagEntity) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DELETE VERTEX \"" + t.VID() + "\";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteVertexes(ts ...TagEntity) (*nebulago.ResultSet, bool, error) {
	if len(ts) == 0 {
		return nil, false, errors.New("no tags")
	}

	vids := make([]string, len(ts))
	for i, t := range ts {
		vids[i] = "\"" + t.VID() + "\""
	}

	command := []string{
		"USE " + ns.Name + ";",
		"DELETE VERTEX \"" + strings.Join(vids, ", ") + "\";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteVertexByTag(t TagEntity) (bool, error) {
	return ns.DeleteVertexByVertexQuery(fmt.Sprintf("lookup on %s yield id(vertex) as vid", t.GetTagName()))
}

func (ns *Space) DeleteVertexByVertexQuery(vertexQuery string) (bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		fmt.Sprintf("%s | delete vertex $-.vid;", vertexQuery),
	}

	_, ok, err := ns.Execute(strings.Join(command, ""))

	return ok, err
}

func (ns *Space) DeleteVertexWithEdge(t TagEntity) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DELETE VERTEX \"" + t.VID() + "\" WITH EDGE;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteVertexWithEdgeByTagName(tagName string) (*nebulago.ResultSet, bool, error) {
	return ns.DeleteVertexWithEDGEByVertexQuery(fmt.Sprintf("lookup on %s yield id(vertex) as vid", tagName))
}

func (ns *Space) DeleteVertexWithEDGEByVertexQuery(vertexQuery string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		fmt.Sprintf("%s | delete vertex $-.vid WITH EDGE;", vertexQuery),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowEdges() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW EDGES;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) CreateEdge(edge *EdgeSchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		edge.CreateString(),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropEdge(edgeName string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DROP EDGE IF EXISTS " + edgeName + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DescribeEdge(edge string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DESCRIBE EDGE " + edge + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) AddEdgeProperty(edge string, property *EdgePropertySchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " ADD (" + property.String() + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) AddEdgeProperties(edge string, properties []*EdgePropertySchema) (*nebulago.ResultSet, bool, error) {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " ADD (" + strings.Join(propertiesString, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ChangeEdgeProperty(edge string, property *EdgePropertySchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " CHANGE (" + property.String() + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ChangeEdgeProperties(edge string, properties []*EdgePropertySchema) (*nebulago.ResultSet, bool, error) {
	propertiesString := make([]string, len(properties))
	for i, prop := range properties {
		propertiesString[i] = prop.String()
	}

	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " CHANGE (" + strings.Join(propertiesString, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropEdgeProperty(edge string, property string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " DROP (" + property + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropEdgeProperties(edge string, properties []string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"ALTER EDGE " + edge + " DROP (" + strings.Join(properties, ", ") + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowEdgeIndexes() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW EDGE INDEXES;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowEdgeIndexesByEdgeName(edgeName string) ([]string, bool, error) {
	resultSet, ok, err := ns.ShowEdgeIndexes()

	if !ok {
		return nil, false, err
	}

	result := make([]string, 0)

	idxNames, err := resultSet.GetValuesByColName("Index Name")

	if err != nil {
		return nil, false, err
	}

	for _, idxName := range idxNames {
		idxn, err := idxName.AsString()
		if err != nil {
			return nil, false, err
		}

		if strings.HasPrefix(idxn, getEdgeIndexPrefix(edgeName)) {
			result = append(result, idxn)
		}
	}

	return result, true, nil
}

func (ns *Space) CreateEdgeIndex(edgeIndex *EdgeIndexSchema) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		edgeIndex.CreateIndexString(),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropEdgeIndex(indexName ...string) (*nebulago.ResultSet, bool, error) {
	command := make([]string, len(indexName)+1)
	command[0] = "USE " + ns.Name + ";"
	for i, idx := range indexName {
		command[i+1] = "DROP EDGE INDEX IF EXISTS " + idx + ";"
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DropEdgeIndexByEdgeName(edgeName string) (*nebulago.ResultSet, bool, error) {
	idxNames, ok, err := ns.ShowEdgeIndexesByEdgeName(edgeName)
	if !ok {
		return nil, false, err
	}

	return ns.DropEdgeIndex(idxNames...)
}

func (ns *Space) DescribeEdgeIndex(indexName string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DESCRIBE EDGE INDEX " + indexName + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) RebuildEdgeIndex(indexName string) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"REBUILD EDGE INDEX " + indexName + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) ShowEdgeIndexStatus() (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"SHOW EDGE INDEX STATUS;",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) CreateEdgeWithIndexes(edge *EdgeSchema) (bool, error) {
	_, ok, err := ns.CreateEdge(edge)

	if !ok {
		return false, err
	}

	for _, idx := range edge.Indexes {
		_, ok, err = ns.CreateEdgeIndex(idx)

		if !ok {
			return false, err
		}
	}

	return true, nil
}

func (ns *Space) DropEdgeWithIndexes(edge string) (bool, error) {

	_, ok, err := ns.DropEdgeIndexByEdgeName(edge)

	if !ok {
		return ok, err
	}

	_, ok, err = ns.DropEdge(edge)

	if !ok {
		return ok, err
	}

	return true, nil
}

func (ns *Space) RebuildEdgeWithIndexes(edge *EdgeSchema) (bool, error) {
	ok, err := ns.DropEdgeWithIndexes(edge.Name)

	if !ok {
		return ok, err
	}

	return ns.CreateEdgeWithIndexes(edge)
}

func (ns *Space) InsertEdge(e EdgeEntity) (*nebulago.ResultSet, bool, error) {
	pns, pvs := GetInsertPropertiesNamesAndValuesString(e)
	command := []string{
		"USE " + ns.Name + ";",
		"INSERT EDGE IF NOT EXISTS " + e.GetEdgeName() + "(" + pns + ") VALUES " + e.EID() + ":(" + pvs + ");",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) InsertEdges(es ...EdgeEntity) (*nebulago.ResultSet, bool, error) {
	if len(es) == 0 {
		return nil, false, errors.New("no edges")
	}

	pns, pvs := make([]string, len(es)), make([]string, len(es))

	for i, e := range es {
		pn, pv := GetInsertPropertiesNamesAndValuesString(e)
		pns[i] = pn
		pvs[i] = e.EID() + ":(" + pv + ")"
	}

	command := []string{
		"USE " + ns.Name + ";",
		"INSERT EDGE IF NOT EXISTS " + es[0].GetEdgeName() + "(" + pns[0] + ") VALUES " + strings.Join(pvs, ", ") + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) BatchInsertEdges(batch int, es []EdgeEntity) (bool, error) {
	if len(es) == 0 {
		return false, errors.New("no edges")
	}

	chunk := lo.Chunk(es, batch)

	for i, c := range chunk {
		_, ok, err := ns.InsertEdges(c...)

		if !ok {
			return false, errors.New(fmt.Sprintf("insert batch %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
		}
	}

	return true, nil
}

func (ns *Space) UpdateEdge(e EdgeEntity) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		GetEdgeUpdateString(e),
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) UpdateEdges(es ...EdgeEntity) (*nebulago.ResultSet, bool, error) {
	if len(es) == 0 {
		return nil, false, errors.New("no edges")
	}

	command := make([]string, len(es)+1)
	command[0] = "USE " + ns.Name + ";"
	for i, t := range es {
		command[i+1] = GetEdgeUpdateString(t)
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) BatchUpdateEdges(batch int, es []EdgeEntity) (bool, error) {
	if len(es) == 0 {
		return false, errors.New("no edges")
	}

	chunk := lo.Chunk(es, batch)

	for i, c := range chunk {
		_, ok, err := ns.UpdateEdges(c...)

		if !ok {
			return false, errors.New(fmt.Sprintf("update batch %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
		}
	}

	return true, nil
}

func (ns *Space) UpsertEdge(e EdgeEntity) (*nebulago.ResultSet, bool, error) {
	pns, pvs := GetUpdatePropertiesNamesAndValuesString(e)

	command := []string{
		"USE " + ns.Name + ";",
		"UPSERT EDGE ON " + e.GetEdgeName() + " " + e.EID() + " SET " + pvs + " YIELD " + pns + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteEdge(e EdgeEntity) (*nebulago.ResultSet, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		"DELETE EDGE " + e.GetEdgeName() + " " + e.EID() + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteEdges(es ...EdgeEntity) (*nebulago.ResultSet, bool, error) {
	if len(es) == 0 {
		return nil, false, errors.New("no edges")
	}

	eids := make([]string, len(es))
	for i, e := range es {
		eids[i] = e.EID()
	}

	command := []string{
		"USE " + ns.Name + ";",
		"DELETE EDGE " + es[0].GetEdgeName() + " " + strings.Join(eids, ", ") + ";",
	}

	return ns.Execute(strings.Join(command, ""))
}

func (ns *Space) DeleteEdgeByEdge(e EdgeEntity) (bool, error) {
	return ns.DeleteEdgeByEdgeQuery(e.GetEdgeName(), fmt.Sprintf("lookup on %s yield src(edge) as src, dst(edge) as dst", e.GetEdgeName()))
}

func (ns *Space) DeleteEdgeByEdgeQuery(edgeName string, edgeQuery string) (bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		fmt.Sprintf("%s | delete edge %s  $-.src -> $-.dst", edgeQuery, edgeName),
	}

	_, ok, err := ns.Execute(strings.Join(command, ""))

	return ok, err
}
