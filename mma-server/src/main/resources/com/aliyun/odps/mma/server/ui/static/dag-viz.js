var VizConstants = {
  svgMarginX: 16,
  svgMarginY: 16,
  stageSep: 40,
  graphPrefix: "graph_",
  nodePrefix: "node_",
  clusterPrefix: "cluster_"
};

var TaskPageVizConstants = {
  clusterLabelSize: 14,
  stageClusterLabelSize: 14,
  rankSep: 40
};

/*
 * Return "expand-dag-viz-arrow-job" if forJob is true.
 * Otherwise, return "expand-dag-viz-arrow-stage".
 */
function expandDagVizArrowKey() {
  return "expand-dag-viz-arrow-task";
}

/*
 * Show or hide the RDD DAG visualization.
 *
 * The graph is only rendered the first time this is called.
 * This is the narrow interface called from the Scala UI code.
 */
function toggleDagViz() {
  var status = window.localStorage.getItem(expandDagVizArrowKey()) == "true";
  status = !status;

  var arrowSelector = ".expand-dag-viz-arrow";
  $(arrowSelector).toggleClass('arrow-closed');
  $(arrowSelector).toggleClass('arrow-open');
  var shouldShow = $(arrowSelector).hasClass("arrow-open");
  if (shouldShow) {
    var shouldRender = graphContainer().select("*").empty();
    if (shouldRender) {
      renderDagViz();
    }
    graphContainer().style("display", "block");
  } else {
    // Save the graph for later so we don't have to render it again
    graphContainer().style("display", "none");
  }

  window.localStorage.setItem(expandDagVizArrowKey(), "" + status);
}

$(function (){
  if ($("#task-dag-viz").length &&
      window.localStorage.getItem(expandDagVizArrowKey(false)) == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem(expandDagVizArrowKey(false), "false");
    toggleDagViz();
  }
});

/*
 * Render the RDD DAG visualization.
 *
 * Input DOM hierarchy:
 *   div#dag-viz-metadata >
 *   div.stage-metadata >
 *   div.[dot-file | incoming-edge | outgoing-edge]
 *
 * Output DOM hierarchy:
 *   div#dag-viz-graph >
 *   svg >
 *   g.cluster_stage_[stageId]
 *
 * Note that the input metadata is populated by o.a.s.ui.UIUtils.showDagViz.
 * Any changes in the input format here must be reflected there.
 */
function renderDagViz() {

  // If there is not a dot file to render, fail fast and report error
  if (metadataContainer().empty() ||
      metadataContainer().selectAll("div").empty()) {
    var message = "<b>No visualization information available</b>";
    graphContainer().append("div").attr("id", "empty-dag-viz-message").html(message);
    return;
  }

  // Render
  var svg = graphContainer().append("svg").attr("class", "task");
  renderDagVizForTask(svg);

  // Find cached RDDs and mark them as such
  metadataContainer().selectAll(".cached-rdd").each(function(v) {
    var rddId = d3.select(this).text().trim();
    var nodeId = VizConstants.nodePrefix + rddId;
    svg.selectAll("g." + nodeId).classed("cached", true);
  });

  resizeSvg(svg);
  interpretLineBreak(svg);
}

/* Render the RDD DAG visualization on the stage page. */
function renderDagVizForTask(svgContainer) {
  var metadata = metadataContainer().select(".task-metadata");
  var dot = metadata.select(".dot-file").text().trim();
  var containerId = VizConstants.graphPrefix + metadata.attr("task-id");
  var container = svgContainer.append("g").attr("id", containerId);
  renderDot(dot, container, false);

  // Round corners on rectangles
  svgContainer
  .selectAll("rect")
  .attr("rx", "5")
  .attr("ry", "5");
}

/*
 * Render the RDD DAG visualization on the job page.
 *
 * Due to limitations in dagre-d3, each stage is rendered independently so that
 * we have more control on how to position them. Unfortunately, this means we
 * cannot rely on dagre-d3 to render edges that cross stages and must render
 * these manually on our own.
 */
function renderDagVizForJob(svgContainer) {
  var crossStageEdges = [];

  // Each div.stage-metadata contains the information needed to generate the graph
  // for a stage. This includes the DOT file produced from the appropriate UI listener,
  // any incoming and outgoing edges, and any cached RDDs that belong to this stage.
  metadataContainer().selectAll(".stage-metadata").each(function(d, i) {
    var metadata = d3.select(this);
    var dot = metadata.select(".dot-file").text();
    var stageId = metadata.attr("stage-id");
    var containerId = VizConstants.graphPrefix + stageId;
    var isSkipped = metadata.attr("skipped") == "true";
    var container;
    if (isSkipped) {
      container = svgContainer
      .append("g")
      .attr("id", containerId)
      .attr("skipped", "true");
    } else {
      // Link each graph to the corresponding stage page (TODO: handle stage attempts)
      // Use the link from the stage table so it also works for the history server
      var attemptId = 0
      var stageLink = d3.select("#stage-" + stageId + "-" + attemptId)
      .select("a.name-link")
      .attr("href");
      container = svgContainer
      .append("a")
      .attr("xlink:href", stageLink)
      .attr("onclick", "window.localStorage.setItem(expandDagVizArrowKey(false), true)")
      .append("g")
      .attr("id", containerId);
    }

    // Now we need to shift the container for this stage so it doesn't overlap with
    // existing ones, taking into account the position and width of the last stage's
    // container. We do not need to do this for the first stage of this job.
    if (i > 0) {
      var existingStages = svgContainer.selectAll("g.cluster.stage")
      if (!existingStages.empty()) {
        var lastStage = d3.select(existingStages[0].pop());
        var lastStageWidth = toFloat(lastStage.select("rect").attr("width"));
        var lastStagePosition = getAbsolutePosition(lastStage);
        var offset = lastStagePosition.x + lastStageWidth + VizConstants.stageSep;
        container.attr("transform", "translate(" + offset + ", 0)");
      }
    }

    // Actually render the stage
    renderDot(dot, container, true);

    // Mark elements as skipped if appropriate. Unfortunately we need to mark all
    // elements instead of the parent container because of CSS override rules.
    if (isSkipped) {
      container.selectAll("g").classed("skipped", true);
    }

    // Round corners on rectangles
    container
    .selectAll("rect")
    .attr("rx", "4")
    .attr("ry", "4");

    // If there are any incoming edges into this graph, keep track of them to render
    // them separately later. Note that we cannot draw them now because we need to
    // put these edges in a separate container that is on top of all stage graphs.
    metadata.selectAll(".incoming-edge").each(function(v) {
      var edge = d3.select(this).text().trim().split(","); // e.g. 3,4 => [3, 4]
      crossStageEdges.push(edge);
    });
  });

  addTooltipsForRDDs(svgContainer);
  drawCrossStageEdges(crossStageEdges, svgContainer);
}

/* Render the dot file as an SVG in the given container. */
function renderDot(dot, container) {
  var escaped_dot = dot
  .replace(/&lt;/g, "<")
  .replace(/&gt;/g, ">")
  .replace(/&quot;/g, "\"");
  var g = graphlibDot.read(escaped_dot);
  var renderer = new dagreD3.render();
  preprocessGraphLayout(g);
  renderer(container, g);

  // Find the stage cluster and mark it for styling and post-processing
  container.selectAll("g.cluster[name^=\"Action \"]").classed("task", true);
}

/* -------------------- *
 * | Helper functions | *
 * -------------------- */

// Helper d3 accessors
function graphContainer() { return d3.select("#dag-viz-graph"); }
function metadataContainer() { return d3.select("#dag-viz-metadata"); }

/*
 * Helper function to pre-process the graph layout.
 * This step is necessary for certain styles that affect the positioning
 * and sizes of graph elements, e.g. padding, font style, shape.
 */
function preprocessGraphLayout(g) {
  var nodes = g.nodes();
  for (var i = 0; i < nodes.length; i++) {
    var isCluster = g.children(nodes[i]).length > 0;
    if (!isCluster) {
      var node = g.node(nodes[i]);
      node.labelStyle = "font-size: 12px";
      node.padding = "5";
    }
  }
  // Curve the edges
  var edges = g.edges();
  for (var j = 0; j < edges.length; j++) {
    var edge = g.edge(edges[j]);
    edge.lineInterpolate = "basis";
  }
  // Adjust vertical separation between nodes
  g.graph().rankSep = TaskPageVizConstants.rankSep;
}

/*
 * Helper function to size the SVG appropriately such that all elements are displayed.
 * This assumes that all outermost elements are clusters (rectangles).
 */
function resizeSvg(svg) {
  var allClusters = svg.selectAll("g.cluster rect")[0];
  var startX = -VizConstants.svgMarginX +
      toFloat(d3.min(allClusters, function(e) {
        return getAbsolutePosition(d3.select(e)).x;
      }));
  var startY = -VizConstants.svgMarginY +
      toFloat(d3.min(allClusters, function(e) {
        return getAbsolutePosition(d3.select(e)).y;
      }));
  var endX = VizConstants.svgMarginX +
      toFloat(d3.max(allClusters, function(e) {
        var t = d3.select(e);
        return getAbsolutePosition(t).x + toFloat(t.attr("width"));
      }));
  var endY = VizConstants.svgMarginY +
      toFloat(d3.max(allClusters, function(e) {
        var t = d3.select(e);
        return getAbsolutePosition(t).y + toFloat(t.attr("height"));
      }));
  var width = endX - startX;
  var height = endY - startY;
  svg.attr("viewBox", startX + " " + startY + " " + width + " " + height)
  .attr("width", width)
  .attr("height", height);
}

/*
 * Helper function to interpret line break for tag 'tspan'.
 * For tag 'tspan', line break '/n' is display in UI as raw for both stage page and job page,
 * here this function is to enable line break.
 */
function interpretLineBreak(svg) {
  var allTSpan = svg.selectAll("tspan").each(function() {
    node = d3.select(this);
    var original = node[0][0].innerHTML;
    if (original.indexOf("\\n") != -1) {
      var arr = original.split("\\n");
      var newNode = this.cloneNode(this);

      node[0][0].innerHTML = arr[0];
      newNode.innerHTML = arr[1];

      this.parentNode.appendChild(newNode);
    }
  });
}

/*
 * (Job page only) Helper function to draw edges that cross stage boundaries.
 * We need to do this manually because we render each stage separately in dagre-d3.
 */
function drawCrossStageEdges(edges, svgContainer) {
  if (edges.length == 0) {
    return;
  }
  // Draw the paths first
  var edgesContainer = svgContainer.append("g").attr("id", "cross-stage-edges");
  for (var i = 0; i < edges.length; i++) {
    var fromRDDId = edges[i][0];
    var toRDDId = edges[i][1];
    connectRDDs(fromRDDId, toRDDId, edgesContainer, svgContainer);
  }
  // Now draw the arrows by borrowing the arrow marker generated by dagre-d3
  var dagreD3Marker = svgContainer.select("g.edgePaths marker");
  if (!dagreD3Marker.empty()) {
    svgContainer
    .append(function() { return dagreD3Marker.node().cloneNode(true); })
    .attr("id", "marker-arrow");
    svgContainer.selectAll("g > path").attr("marker-end", "url(#marker-arrow)");
    svgContainer.selectAll("g.edgePaths def").remove(); // We no longer need these
  }
}

/*
 * (Job page only) Helper function to compute the absolute
 * position of the specified element in our graph.
 */
function getAbsolutePosition(d3selection) {
  if (d3selection.empty()) {
    throw "Attempted to get absolute position of an empty selection.";
  }
  var obj = d3selection;
  var _x = toFloat(obj.attr("x")) || 0;
  var _y = toFloat(obj.attr("y")) || 0;
  while (!obj.empty()) {
    var transformText = obj.attr("transform");
    if (transformText) {
      var translate = d3.transform(transformText).translate;
      _x += toFloat(translate[0]);
      _y += toFloat(translate[1]);
    }
    // Climb upwards to find how our parents are translated
    obj = d3.select(obj.node().parentNode);
    // Stop when we've reached the graph container itself
    if (obj.node() == graphContainer().node()) {
      break;
    }
  }
  return { x: _x, y: _y };
}

/* (Job page only) Helper function to connect two RDDs with a curved edge. */
function connectRDDs(fromRDDId, toRDDId, edgesContainer, svgContainer) {
  var fromNodeId = VizConstants.nodePrefix + fromRDDId;
  var toNodeId = VizConstants.nodePrefix + toRDDId;
  var fromPos = getAbsolutePosition(svgContainer.select("g." + fromNodeId));
  var toPos = getAbsolutePosition(svgContainer.select("g." + toNodeId));

  // On the job page, RDDs are rendered as dots (circles). When rendering the path,
  // we need to account for the radii of these circles. Otherwise the arrow heads
  // will bleed into the circle itself.
  var delta = toFloat(svgContainer
  .select("g.node." + toNodeId)
  .select("circle")
  .attr("r"));
  if (fromPos.x < toPos.x) {
    fromPos.x += delta;
    toPos.x -= delta;
  } else if (fromPos.x > toPos.x) {
    fromPos.x -= delta;
    toPos.x += delta;
  }

  var points;
  if (fromPos.y == toPos.y) {
    // If they are on the same rank, curve the middle part of the edge
    // upward a little to avoid interference with things in between
    // e.g.       _______
    //      _____/       \_____
    points = [
      [fromPos.x, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.2, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.3, fromPos.y - 20],
      [fromPos.x + (toPos.x - fromPos.x) * 0.7, fromPos.y - 20],
      [fromPos.x + (toPos.x - fromPos.x) * 0.8, toPos.y],
      [toPos.x, toPos.y]
    ];
  } else {
    // Otherwise, draw a curved edge that flattens out on both ends
    // e.g.       _____
    //           /
    //          |
    //    _____/
    points = [
      [fromPos.x, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.4, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.6, toPos.y],
      [toPos.x, toPos.y]
    ];
  }

  var line = d3.svg.line().interpolate("basis");
  edgesContainer.append("path").datum(points).attr("d", line);
}

/*
 * Replace `/n` with `<br/>`
 */
function replaceLineBreak(str) {
  return str.replace("\\n", "<br/>");
}

/* (Job page only) Helper function to add tooltips for RDDs. */
function addTooltipsForRDDs(svgContainer) {
  svgContainer.selectAll("g.node").each(function() {
    var node = d3.select(this);
    var tooltipText = replaceLineBreak(node.attr("name"));
    if (tooltipText) {
      node.select("circle")
      .attr("data-toggle", "tooltip")
      .attr("data-placement", "bottom")
      .attr("data-html", "true") // to interpret line break, tooltipText is showing <circle> title
      .attr("title", tooltipText);
    }
    // Link tooltips for all nodes that belong to the same RDD
    node.on("mouseenter", function() { triggerTooltipForRDD(node, true); });
    node.on("mouseleave", function() { triggerTooltipForRDD(node, false); });
  });

  $("[data-toggle=tooltip]")
  .filter("g.node circle")
  .tooltip({ container: "body", trigger: "manual" });
}

/*
 * (Job page only) Helper function to show or hide tooltips for all nodes
 * in the graph that refer to the same RDD the specified node represents.
 */
function triggerTooltipForRDD(d3node, show) {
  var classes = d3node.node().classList;
  for (var i = 0; i < classes.length; i++) {
    var clazz = classes[i];
    var isRDDClass = clazz.indexOf(VizConstants.nodePrefix) == 0;
    if (isRDDClass) {
      graphContainer().selectAll("g." + clazz).each(function() {
        var circle = d3.select(this).select("circle").node();
        var showOrHide = show ? "show" : "hide";
        $(circle).tooltip(showOrHide);
      });
    }
  }
}

/* Helper function to convert attributes to numeric values. */
function toFloat(f) {
  if (f) {
    return parseFloat(f.toString().replace(/px$/, ""));
  } else {
    return f;
  }
}

