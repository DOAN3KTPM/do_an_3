function init() {

    var $ = go.GraphObject.make;  // for conciseness in defining templates

    myDiagram =
        $(go.Diagram, "myDiagramDiv",  // must name or refer to the DIV HTML element
            {
                initialContentAlignment: go.Spot.Center,
                allowDrop: true,  // must be true to accept drops from the Palette
                "LinkDrawn": showLinkLabel,  // this DiagramEvent listener is defined below
                "LinkRelinked": showLinkLabel,
                "animationManager.duration": 800, // slightly longer than default (600ms) animation
                "undoManager.isEnabled": true  // enable undo & redo
            });

    // when the document is modified, add a "*" to the title and enable the "Save" button
    myDiagram.addDiagramListener("ObjectDoubleClicked", function (ev) {
        let data = ev.subject.part.data
        switch (data.category) {
            case 'MONGODB': {
                let models = myDiagram.model;
                let arrayLinks = JSON.parse(models.toJson()).linkDataArray

                let modelLink = arrayLinks.filter(link => link.to == data.key
            )
                ;

                let keyLink = null;
                if (typeof modelLink !== 'undefined' && modelLink.length > 0) {
                    keyLink = modelLink[0].from
                    //
                    let collectionNames = new Array()
                    let hostname, dbname, port = "";

                    var dataLink = models.findNodeDataForKey(keyLink);
                    collectionNames = dataLink.collectionNames

                    hostname = dataLink.hostname
                    dbname = dataLink.dbname
                    username = dataLink.username
                    password = dataLink.password
                    port = dataLink.port
                    loadMySqlGetData(hostname, dbname, username, password, port, collectionNames)
                } else {
                    let hostname = data.hostname || 'localhost'
                    let port = data.port || '27017'
                    let dbname = data.dbname || ''
                    let collectionNames = data.collectionNames ? data.collectionNames : []
                    Cookies.set("showJsonMongodb", {hostname,port,dbname,collectionNames})
                    loadMongodbModal(data.key, collectionNames)
                }
                break;
            }
            case 'SQL': {
                let models = myDiagram.model;
                let arrayLinks = JSON.parse(models.toJson()).linkDataArray

                let modelLink = arrayLinks.filter(link => link.to == data.key
            )
                ;

                let keyLink = null;
                if (typeof modelLink !== 'undefined' && modelLink.length > 0) {
                    keyLink = modelLink[0].from
                    //
                    let collectionNames = new Array()
                    let hostname, dbname, port = "";

                    var dataLink = models.findNodeDataForKey(keyLink);
                    collectionNames = dataLink.collectionNames

                    hostname = dataLink.hostname
                    dbname = dataLink.dbname
                    port = dataLink.port
                    loadMongodbGetData(hostname, dbname, port, collectionNames)
                } else {
                    let hostname = data.hostname || 'localhost'
                    let port = data.port || '3306'
                    let username = data.username || 'root'
                    let password = data.password || ''
                    let dbname = data.dbname || ''
                    let collectionNames = data.collectionNames ? data.collectionNames : []
                    Cookies.set("showJsonMysql", {hostname,port,username,password,dbname, collectionNames})
                    loadMysqlModal(data.key, collectionNames)
                }
                // send host name port collectionName to query
                // get meta data


                break;
            }
        }

        console.log(ev); //Successfully logs the node you clicked.
        console.log(ev.subject.ie); //Successfully logs the node's name.
        // console.log(myDiagram.model.toJson())
        // model.findNodeDataForKey(1234);

    });

    myDiagram.addDiagramListener("Modified", function (e) {
        var button = document.getElementById("SaveButton");
        if (button) button.disabled = !myDiagram.isModified;
        var idx = document.title.indexOf("*");
        if (myDiagram.isModified) {
            if (idx < 0) document.title += "*";
        } else {
            if (idx >= 0) document.title = document.title.substr(0, idx);
        }
    });

    // helper definitions for node templates

    function nodeStyle() {
        return [
            // The Node.location comes from the "loc" property of the node data,
            // converted by the Point.parse static method.
            // If the Node.location is changed, it updates the "loc" property of the node data,
            // converting back using the Point.stringify static method.
            new go.Binding("location", "loc", go.Point.parse).makeTwoWay(go.Point.stringify),
            {
                // the Node.location is at the center of each node
                locationSpot: go.Spot.Center,
                //isShadowed: true,
                //shadowColor: "#888",
                // handle mouse enter/leave events to show/hide the ports
                mouseEnter: function (e, obj) {
                    showPorts(obj.part, true);
                },
                mouseLeave: function (e, obj) {
                    showPorts(obj.part, false);
                }
            }
        ];
    }

    // Define a function for creating a "port" that is normally transparent.
    // The "name" is used as the GraphObject.portId, the "spot" is used to control how links connect
    // and where the port is positioned on the node, and the boolean "output" and "input" arguments
    // control whether the user can draw links from or to the port.
    function makePort(name, spot, output, input) {
        // the port is basically just a small circle that has a white stroke when it is made visible
        return $(go.Shape, "Circle",
            {
                fill: "transparent",
                stroke: null,  // this is changed to "white" in the showPorts function
                desiredSize: new go.Size(8, 8),
                alignment: spot, alignmentFocus: spot,  // align the port on the main Shape
                portId: name,  // declare this object to be a "port"
                fromSpot: spot, toSpot: spot,  // declare where links may connect at this port
                fromLinkable: output, toLinkable: input,  // declare whether the user may draw links to/from here
                cursor: "pointer"  // show a different cursor to indicate potential link point
            });
    }

    // define the Node templates for regular nodes

    var lightText = 'whitesmoke';

    // myDiagram.nodeTemplateMap.add("Start",  // the default category
    //     $(go.Node, "Spot", nodeStyle(),
    //         // the main object is a Panel that surrounds a TextBlock with a rectangular Shape
    //         $(go.Panel, "Auto",
    //             $(go.Picture,
    //                 {desiredSize: new go.Size(40, 40), source: "assets/assets/images/mongodb.png"}),
    //         ),
    //         // four named ports, one on each side:
    //         makePort("T", go.Spot.Top, false, true),
    //         makePort("L", go.Spot.Left, true, true),
    //         makePort("R", go.Spot.Right, true, true),
    //         makePort("B", go.Spot.Bottom, true, false)
    //     ));
    myDiagram.nodeTemplateMap.add("MONGODB",  // the default category
        $(go.Node, "Spot", nodeStyle(),
            // the main object is a Panel that surrounds a TextBlock with a rectangular Shape
            $(go.Panel, "Auto",
                $(go.Picture,
                    {desiredSize: new go.Size(40, 40), source: "assets/assets/images/mongodb.png"}),
            ),
            // four named ports, one on each side:
            makePort("T", go.Spot.Top, false, true),
            makePort("L", go.Spot.Left, true, true),
            makePort("R", go.Spot.Right, true, true),
            makePort("B", go.Spot.Bottom, true, false)
        ));
    myDiagram.nodeTemplateMap.add("SQL",
        $(go.Node, "Spot", nodeStyle(),
            $(go.Panel, "Auto",
                $(go.Picture,
                    {desiredSize: new go.Size(40, 40), source: "assets/assets/images/sql.png"})
            ),
            makePort("T", go.Spot.Top, false, true),
            makePort("L", go.Spot.Left, true, true),
            makePort("R", go.Spot.Right, true, true),
            makePort("B", go.Spot.Bottom, true, false)
        ));

    // replace the default Link template in the linkTemplateMap
    myDiagram.linkTemplate =
        $(go.Link,  // the whole link panel
            {
                routing: go.Link.AvoidsNodes,
                curve: go.Link.JumpOver,
                corner: 5, toShortLength: 4,
                relinkableFrom: true,
                relinkableTo: true,
                reshapable: true,
                resegmentable: true,
                // mouse-overs subtly highlight links:
                mouseEnter: function (e, link) {
                    link.findObject("HIGHLIGHT").stroke = "rgba(30,144,255,0.2)";
                },
                mouseLeave: function (e, link) {
                    link.findObject("HIGHLIGHT").stroke = "transparent";
                }
            },
            new go.Binding("points").makeTwoWay(),
            $(go.Shape,  // the highlight shape, normally transparent
                {isPanelMain: true, strokeWidth: 8, stroke: "transparent", name: "HIGHLIGHT"}),
            $(go.Shape,  // the link path shape
                {isPanelMain: true, stroke: "gray", strokeWidth: 2}),
            $(go.Shape,  // the arrowhead
                {toArrow: "standard", stroke: null, fill: "gray"}),
            $(go.Panel, "Auto",  // the link label, normally not visible
                {visible: false, name: "LABEL", segmentIndex: 2, segmentFraction: 0.5},
                new go.Binding("visible", "visible").makeTwoWay(),
                $(go.Shape, "RoundedRectangle",  // the label shape
                    {fill: "#F8F8F8", stroke: null}),
                $(go.TextBlock, "Yes",  // the label
                    {
                        textAlign: "center",
                        font: "10pt helvetica, arial, sans-serif",
                        stroke: "#333333",
                        editable: true
                    },
                    new go.Binding("text").makeTwoWay())
            )
        );

    // Make link labels visible if coming out of a "conditional" node.
    // This listener is called by the "LinkDrawn" and "LinkRelinked" DiagramEvents.
    function showLinkLabel(e) {
        var label = e.subject.findObject("LABEL");
        if (label !== null) label.visible = (e.subject.fromNode.data.figure === "Diamond");
    }

    // temporary links used by LinkingTool and RelinkingTool are also orthogonal:
    myDiagram.toolManager.linkingTool.temporaryLink.routing = go.Link.Orthogonal;
    myDiagram.toolManager.relinkingTool.temporaryLink.routing = go.Link.Orthogonal;

    load();  // load an initial diagram from some JSON text

    // initialize the Palette that is on the left side of the page
    myPalette =
        $(go.Palette, "myPaletteDiv",  // must name or refer to the DIV HTML element
            {
                "animationManager.duration": 800, // slightly longer than default (600ms) animation
                nodeTemplateMap: myDiagram.nodeTemplateMap,  // share the templates used by myDiagram
                model: new go.GraphLinksModel([  // specify the contents of the Palette
                    {category: "MONGODB", text: "MONGODB"},
                    {category: "SQL", text: "SQL",}
                ])
            });

    // The following code overrides GoJS focus to stop the browser from scrolling
    // the page when either the Diagram or Palette are clicked or dragged onto.
    myDiagram.model = go.Model.fromJson(JSON.parse(Cookies.get('modelData')));

    function customFocus() {
        var x = window.scrollX || window.pageXOffset;
        var y = window.scrollY || window.pageYOffset;
        go.Diagram.prototype.doFocus.call(this);
        window.scrollTo(x, y);
    }

    myDiagram.doFocus = customFocus;
    myPalette.doFocus = customFocus;


} // end init

// Make all ports on a node visible when the mouse is over the node
function showPorts(node, show) {
    var diagram = node.diagram;
    if (!diagram || diagram.isReadOnly || !diagram.allowLink) return;
    node.ports.each(function (port) {
        port.stroke = (show ? "white" : null);
    });
}


// Show the diagram's model in JSON format that the user may edit
function save() {


    document.getElementById("mySavedModel").value = myDiagram.model.toJson();

    download(myDiagram.model.toJson(), "loadingData.json", "text/plain")
    myDiagram.isModified = false;
}

function download(data, filename, type) {
    var file = new Blob([data], {type: type});
    if (window.navigator.msSaveOrOpenBlob) // IE10+
        window.navigator.msSaveOrOpenBlob(file, filename);
    else { // Others
        var a = document.createElement("a"),
            url = URL.createObjectURL(file);
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        setTimeout(function () {
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        }, 0);
    }
}

function load() {
    myDiagram.model = go.Model.fromJson(document.getElementById("mySavedModel").value);
}

// add an SVG rendering of the diagram at the end of this page
function makeSVG() {
    var svg = myRelate.makeSvg({
        scale: 1
    });
    svg.style.border = "1px solid black";
    obj = document.getElementById("SVGArea");
    obj.appendChild(svg);
    if (obj.children.length > 0) {
        obj.replaceChild(svg, obj.children[0]);
    }
}

$('input[type=file]').change(function () {
    var files = document.getElementById('import').files;

    if (files.length <= 0) {
        return false;
    }

    var fr = new FileReader();

    fr.onload = function (e) {
        $("#mySavedModel").text(e.target.result);
    }

    fr.readAsText(files.item(0));
});

//  load mongodb modal
function loadMongodbModal(key, collectionNames) {
    var jsonMongodb = JSON.parse(Cookies.get("showJsonMongodb"))

    $('#mongodbModal').find('#hostname-mg').val(jsonMongodb.hostname)
    $('#mongodbModal').find('#port-mg').val(jsonMongodb.port)
    $('#mongodbModal').find('#dbname-mg').val(jsonMongodb.dbname)
    $('#mongodbModal').find('.save').attr('data-key', key)
    let html = "";
    for (var name of collectionNames) {
        html += `<div class="collection">
                <input type="checkbox" name="collections" value="${name}" id="${name}" checked> <label for="${name}">${name}</label>
            </div>`
    }
    $('#mongodbModal .collections').html(html)

    $('#mongodbModal').modal('show');

}

//  load mongodb modal
function loadMysqlModal(key, collectionNames) {
    var jsonMysql = JSON.parse(Cookies.get("showJsonMysql"))
    $('#mysqlModal').find('#hostname-mg').val(jsonMysql.hostname)
    $('#mysqlModal').find('#port-mg').val(jsonMysql.port)
    $('#mysqlModal').find('#username-mg').val(jsonMysql.username)
    $('#mysqlModal').find('#pass-mg').val(jsonMysql.password)
    $('#mysqlModal').find('#dbname-mg').val(jsonMysql.dbname)
    $('#mysqlModal').find('.save').attr('data-key', key)
    let html = "";
    for (var name of collectionNames) {
        html += `<div class="collection">
                <input type="checkbox" name="collections" value="${name}" id="${name}" checked> <label for="${name}">${name}</label>
            </div>`
    }
    $('#mysqlModal .collections').html(html)

    $('#mysqlModal').modal('show');
}

// load sql modal
function loadMySqlGetData(hostname, dbname, username, password, port, collectionNames) {

    $.ajax({
        method: "POST",
        url: "/getMetaDataMYSQL",
        data: {
            hostname,
            dbname,
            username,
            password,
            port,
            tables_input: JSON.stringify(collectionNames)
        },
        beforeSend: function() {
            $("#myModalLoad").show();
        },
        complete: function() {
            $("#myModalLoad").hide();
        },
        success: function (data) {
            data_tables = data.tables // global
            let dataCollections = data.tables

            if (dataCollections.length > 0) {
                var html = ""
                for (let table of dataCollections) {
                    html += `
<div class="table">
<h3>${table.name}</h3>
<table class="table table-bordered">
  <thead>
    <tr>
    <th>Primary key <i class="fas fa-key"></i></th>
      <th scope="col">Field name</th>
      <th scope="col">Type</th>
    </tr>
  </thead>
  <tbody>
  `;
                    let i = 0;
                    for (let field of  table.fields) {
                        i++
                        html += `<tr>
                            <th scope="row">`;
                        if(field.name === table.primaryKey) {
                            html += `<input type="radio" class="primary-key" name="${table.name}" value="${field.name}" checked/></th>`
                        } else {
                            html += `<input type="radio" class="primary-key" name="${table.name}" value="${field.name}"/></th>`
                        }

                        html += `<td>${field.name}</td>
                          <td>${field.type}</td>
                        </tr>`
                    }


                    html += `
  </tbody>
</table>
</div>

            `
                }
            }

            $('#sqlModal .modal-body').html(html)

            $('#sqlModal').modal('show');
        }
    })


}

// load sql modal
function loadMongodbGetData(hostname, dbname, port, collectionNames) {

    $.ajax({
        method: "POST",
        url: "/getMetadataOfCollection",
        data: {
            hostname,
            dbname,
            port,
            collectionNames: JSON.stringify(collectionNames)
        },
        beforeSend: function() {
            $("#myModalLoad").show();
        },
        complete: function() {
            $("#myModalLoad").hide();
        },
        success: function (data) {
            data_tables = data.tables // global
            let dataCollections = data.tables

            if (dataCollections.length > 0) {
                var html = ""
                for (let table of dataCollections) {
                    html += `
<div class="table">
<h3>${table.name}</h3>
<table class="table table-bordered">
  <thead>
    <tr>
    <th>Primary key <i class="fas fa-key"></i></th>
      <th scope="col">Field name</th>
      <th scope="col">Type</th>
    </tr>
  </thead>
  <tbody>
  `;
                    let i = 0;
                    for (let field of  table.fields) {
                        i++
                        html += `<tr>
                            <th scope="row">`;
                        if(field.name === table.primaryKey) {
                            html += `<input type="radio" class="primary-key" name="${table.name}" value="${field.name}" checked/></th>`
                        } else {
                            html += `<input type="radio" class="primary-key" name="${table.name}" value="${field.name}"/></th>`
                        }

                        html += `<td>${field.name}</td>
                          <td>${field.type}</td>
                        </tr>`
                    }


                    html += `
  </tbody>
</table>
</div>

            `
                }
            }

            $('#sqlModal .modal-body').html(html)

            $('#sqlModal').modal('show');
        }
    })


}

function saveConfigureMongodb(input) {

    let key = $(input).attr('data-key')

    let parent = $(input).parents('#mongodbModal')
    let hostname = parent.find('#hostname-mg').val()
    let dbname = parent.find('#dbname-mg').val()
    let port = parent.find('#port-mg').val()

    let collectionElements = parent.find('[name="collections"]:checked');
    let collectionNames = new Array();
    for (let element of collectionElements) {
        collectionNames.push($(element).val())
    }
    let nodeData = myDiagram.model.findNodeDataForKey(key);

    nodeData.hostname = hostname
    nodeData.port = port
    nodeData.dbname = dbname
    nodeData.collectionNames = collectionNames
    $('#mongodbModal').modal('hide');
}



function saveConfigureMysql(input) {

    let key = $(input).attr('data-key')

    let parent = $(input).parents('#mysqlModal')
    let hostname = parent.find('#hostname-mg').val()
    let username = parent.find('#username-mg').val()
    let password = parent.find('#pass-mg').val()
    let dbname = parent.find('#dbname-mg').val()

    let port = parent.find('#port-mg').val()
    let collectionElements = parent.find('[name="collections"]:checked');
    let collectionNames = new Array();
    for (let element of collectionElements) {
        collectionNames.push($(element).val())
    }
    let nodeData = myDiagram.model.findNodeDataForKey(key);

    nodeData.hostname = hostname
    nodeData.port = port
    nodeData.dbname = dbname
    nodeData.username = username
    nodeData.password = password
    nodeData.collectionNames = collectionNames

    $('#mysqlModal').modal('hide');
}

function checkConnectMongodb(input) {
    $('#select-all').removeClass('d-none')
    let parent = $(input).parents('#mongodbModal')
    let hostname = parent.find('#hostname-mg').val()
    let dbname = parent.find('#dbname-mg').val()
    let port = parent.find('#port-mg').val()
    $.ajax({
        method: "POST",
        url: "/checkConnectionMGDB",
        data: {
            hostname,
            dbname,
            port
        },
        beforeSend: function() {
            $("#myModalLoad").show();
        },
        complete: function() {
            $("#myModalLoad").hide();
        },
        success: function (data) {
            console.log(data)
            let html = "";
            if (data.status == 500) {
                toastr.error('Kết nối thất bại!')
            } else {
                var collectionNames = data.collectionNames
                console.log(collectionNames)
                let html = ''
                for (var name of collectionNames) {
                    html += `<div class="collection">
                                <input type="checkbox" name="collections" value="${name}" id="${name}"> <label for="${name}">${name}</label>
                            </div>`
                }
                $('.collections').html(html)

                toastr.success('Kết nối thành công!')
            }
        }
    })


}


function loadingMedata() {

}


function checkConnectMysql(input) {
    $('#select-all-1').removeClass('d-none')
    let parent = $(input).parents('#mysqlModal')
    let hostname = parent.find('#hostname-mg').val()
    let dbname = parent.find('#dbname-mg').val()
    let username = parent.find('#username-mg').val()
    let password = parent.find('#pass-mg').val()
    let port = parent.find('#port-mg').val()
    $.ajax({
        method: "POST",
        url: "/checkConnectionMYSQL",
        data: {
            hostname,
            dbname,
            username,
            password,
            port
        },
        beforeSend: function() {
            $("#myModalLoad").show();
        },
        complete: function() {
            $("#myModalLoad").hide();
        },
        success: function (data) {
            console.log(data)
            let html = "";
            if (data.status == 500) {
                toastr.error('Kết nối thất bại!')
            } else {
                var collectionNames = data.tableNames
                console.log(collectionNames)
                let html = ''
                for (var name of collectionNames) {
                    html += `<div class="collection">
                                <input type="checkbox" name="collections" value="${name}" id="${name}"> <label for="${name}">${name}</label>
                            </div>`
                }
                $('.collections').html(html)

                toastr.success('Kết nối thành công!')
            }
        }
    })


}


$('#select_all').click(function () {
    var c = this.checked;
    $(':checkbox').prop('checked', c);
});

$('#select_all_1').click(function () {
    var c = this.checked;
    $(':checkbox').prop('checked', c);
});