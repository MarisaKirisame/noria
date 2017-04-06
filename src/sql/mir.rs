use flow::core::{NodeAddress, DataType};
use mir::{GroupedNodeType, MirNode, MirNodeType};
// TODO(malte): remove if possible
pub use mir::{FlowNode, MirNodeRef, MirQuery};
use ops::join::JoinType;

use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::{SelectStatement, LimitClause, OrderClause};
use sql::query_graph::{QueryGraph, QueryGraphEdge};

use slog;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::vec::Vec;

fn target_columns_from_computed_column(computed_col: &Column) -> &Column {
    use nom_sql::FunctionExpression::*;

    match *computed_col.function
               .as_ref()
               .unwrap()
               .deref() {
        Avg(ref col, _) |
        Count(ref col, _) |
        GroupConcat(ref col, _) |
        Max(ref col) |
        Min(ref col) |
        Sum(ref col, _) => col,
        CountStar => {
            // see comment re COUNT(*) rewriting in make_aggregation_node
            panic!("COUNT(*) should have been rewritten earlier!")
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlToMirConverter {
    base_schemas: HashMap<String, Vec<(usize, Vec<Column>)>>,
    current: HashMap<String, usize>,
    log: slog::Logger,
    nodes: HashMap<(String, usize), MirNodeRef>,
    schema_version: usize,
}

impl Default for SqlToMirConverter {
    fn default() -> Self {
        SqlToMirConverter {
            base_schemas: HashMap::default(),
            current: HashMap::default(),
            log: slog::Logger::root(slog::Discard, None),
            nodes: HashMap::default(),
            schema_version: 0,
        }
    }
}

impl SqlToMirConverter {
    pub fn with_logger(log: slog::Logger) -> Self {
        SqlToMirConverter { log: log, ..Default::default() }
    }

    /// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
    /// vector of conditions that `shortcut` understands.
    fn to_conditions(&self,
                     ct: &ConditionTree,
                     mut columns: &mut Vec<Column>,
                     n: &MirNodeRef)
                     -> Vec<Option<(Operator, DataType)>> {
        // TODO(malte): we only support one level of condition nesting at this point :(
        let l = match *ct.left.as_ref() {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
        let r = match *ct.right.as_ref() {
            ConditionExpression::Base(ConditionBase::Literal(ref l)) => l.clone(),
            _ => unimplemented!(),
        };

        let num_columns = columns.len();
        let mut filters = vec![None; num_columns];

        let f = Some((ct.operator.clone(), DataType::from(r)));
        match n.borrow()
                  .columns()
                  .iter()
                  .position(|c| *c == l) {
            None => {
                columns.push(l);
                filters.push(f);
            }
            Some(cid) => {
                filters[cid] = f;
            }
        }
        filters
    }

    pub fn add_leaf_below(&mut self,
                          leaf: MirNodeRef,
                          name: &str,
                          params: &Vec<Column>)
                          -> MirQuery {
        let columns: Vec<Column> = leaf.borrow()
            .columns()
            .iter()
            .cloned()
            .collect();

        // reuse the previous leaf node
        let parent = MirNode::reuse(leaf, self.schema_version);

        // add an identity node and then another leaf
        let id = MirNode::new(&format!("{}_id", name),
                              self.schema_version,
                              columns.clone(),
                              MirNodeType::Identity,
                              vec![parent.clone()],
                              vec![]);

        let new_leaf = MirNode::new(name,
                                    self.schema_version,
                                    columns,
                                    MirNodeType::Leaf {
                                        node: parent.clone(),
                                        keys: params.clone(),
                                    },
                                    vec![id.clone()],
                                    vec![]);

        // always register lleaves
        self.current.insert(String::from(name), self.schema_version);
        self.nodes.insert((String::from(name), self.schema_version), new_leaf.clone());

        // wrap in a (very short) query to return
        MirQuery {
            name: String::from(name),
            roots: vec![parent],
            leaf: new_leaf,
        }
    }

    pub fn get_flow_node_address(&self, name: &str, version: usize) -> Option<NodeAddress> {
        match self.nodes.get(&(name.to_string(), version)) {
            None => None,
            Some(ref node) => {
                match node.borrow().flow_node {
                    None => None,
                    Some(ref flow_node) => Some(flow_node.address()),
                }
            }
        }
    }

    pub fn get_leaf(&self, name: &str) -> Option<NodeAddress> {
        match self.current.get(name) {
            None => None,
            Some(v) => self.get_flow_node_address(name, *v),
        }
    }

    pub fn named_base_to_mir(&mut self, name: &str, query: &SqlQuery) -> MirQuery {
        match *query {
            SqlQuery::CreateTable(ref ctq) => {
                assert_eq!(name, ctq.table.name);
                let n = self.make_base_node(&name, &ctq.fields, ctq.keys.as_ref());
                let node_id = (String::from(name), self.schema_version);
                if !self.nodes.contains_key(&node_id) {
                    self.nodes.insert(node_id, n.clone());
                    self.current.insert(String::from(name), self.schema_version);
                }
                MirQuery::singleton(name, n)
            }
            SqlQuery::Insert(ref iq) => {
                assert_eq!(name, iq.table.name);
                let (cols, _): (Vec<Column>, Vec<String>) = iq.fields
                    .iter()
                    .cloned()
                    .unzip();
                let n = self.make_base_node(&name, &cols, None);
                let node_id = (String::from(name), self.schema_version);
                if !self.nodes.contains_key(&node_id) {
                    self.nodes.insert(node_id, n.clone());
                    self.current.insert(String::from(name), self.schema_version);
                }
                MirQuery::singleton(name, n)
            }
            _ => panic!("expected base-yielding query!"),
        }
    }

    pub fn named_query_to_mir(&mut self,
                              name: &str,
                              sq: &SelectStatement,
                              qg: &QueryGraph)
                              -> MirQuery {
        let nodes = self.make_nodes_for_selection(&name, sq, qg);
        let mut roots = Vec::new();
        let mut leaves = Vec::new();
        for mn in nodes {
            let node_id = (String::from(mn.borrow().name()), self.schema_version);
            // only add the node if we don't have it registered at this schema version already. If
            // we don't do this, we end up adding the node again for every re-use of it, with
            // increasingly deeper chains of nested `MirNode::Reuse` structures.
            if !self.nodes.contains_key(&node_id) {
                self.nodes.insert(node_id, mn.clone());
            }

            trace!(self.log,
                   "Added MIR node ({}, v{}): {:?}",
                   mn.borrow().name(),
                   self.schema_version,
                   mn);

            if mn.borrow().ancestors().len() == 0 {
                // root
                roots.push(mn.clone());
            }
            if mn.borrow().children().len() == 0 {
                // leaf
                leaves.push(mn);
            }
        }
        assert_eq!(leaves.len(),
                   1,
                   "expected just one leaf! leaves: {:?}",
                   leaves);
        let leaf = leaves.into_iter().next().unwrap();
        self.current.insert(String::from(leaf.borrow().name()), self.schema_version);

        MirQuery {
            name: String::from(name),
            roots: roots,
            leaf: leaf,
        }
    }

    pub fn upgrade_schema(&mut self, new_version: usize) {
        assert!(new_version > self.schema_version);
        self.schema_version = new_version;
    }

    fn make_base_node(&mut self,
                      name: &str,
                      cols: &Vec<Column>,
                      keys: Option<&Vec<TableKey>>)
                      -> MirNodeRef {
        // have we seen a base of this name before?
        if self.base_schemas.contains_key(name) {
            let mut existing_schemas: Vec<(usize, Vec<Column>)> = self.base_schemas[name].clone();
            existing_schemas.sort_by_key(|&(sv, _)| sv);

            for (existing_sv, ref schema) in existing_schemas {
                // TODO(malte): check the keys too
                if schema == cols {
                    // exact match, so reuse the existing base node
                    info!(self.log,
                          "base table for {} already exists with identical schema in version {}; reusing it.",
                          name,
                          existing_sv);
                    let existing_node = self.nodes[&(String::from(name), existing_sv)].clone();
                    return MirNode::reuse(existing_node, self.schema_version);
                } else {
                    // match, but schema is different, so we'll need to either:
                    //  1) reuse the existing node, but add an upgrader for any changes in the column
                    //     set, or
                    //  2) give up and just make a new node
                    error!(self.log,
                           "base table for {} already exists in version {}, but has a different schema!",
                           name,
                           existing_sv);
                }
            }
        }

        // all columns on a base must have the base as their table
        assert!(cols.iter().all(|c| c.table == Some(String::from(name))));

        let primary_keys = match keys {
            None => vec![],
            Some(keys) => {
                keys.iter()
                    .filter_map(|k| match *k {
                                    ref k @ TableKey::PrimaryKey(..) => Some(k),
                                    _ => None,
                                })
                    .collect()
            }
        };
        // TODO(malte): support >1 pkey
        assert!(primary_keys.len() <= 1);

        // remember the schema for this version
        let mut base_schemas = self.base_schemas.entry(String::from(name)).or_insert(Vec::new());
        base_schemas.push((self.schema_version, cols.clone()));

        // make node
        if !primary_keys.is_empty() {
            match **primary_keys.iter().next().unwrap() {
                TableKey::PrimaryKey(ref key_cols) => {
                    debug!(self.log,
                           "Assigning primary key ({}) for base {}",
                           key_cols.iter()
                               .map(|c| c.name.as_str())
                               .collect::<Vec<_>>()
                               .join(", "),
                           name);
                    MirNode::new(name,
                                 self.schema_version,
                                 cols.clone(),
                                 MirNodeType::Base { keys: key_cols.clone() },
                                 vec![],
                                 vec![])
                }
                _ => unreachable!(),
            }
        } else {
            MirNode::new(name,
                         self.schema_version,
                         cols.clone(),
                         MirNodeType::Base { keys: vec![] },
                         vec![],
                         vec![])
        }
    }

    fn make_filter_nodes(&mut self,
                         name: &str,
                         parent: MirNodeRef,
                         predicates: &Vec<ConditionTree>)
                         -> Vec<MirNodeRef> {
        let mut new_nodes = vec![];

        let mut prev_node = parent;
        for (i, cond) in predicates.iter().enumerate() {
            let mut fields = prev_node.borrow()
                .columns()
                .iter()
                .cloned()
                .collect();
            // convert ConditionTree to a chain of Filter operators.
            // TODO(malte): this doesn't handle OR or AND correctly: needs a nested loop
            let filter = self.to_conditions(cond, &mut fields, &prev_node);
            let f_name = format!("{}_f{}", name, i);
            let n = MirNode::new(&f_name,
                                 self.schema_version,
                                 fields,
                                 MirNodeType::Filter { conditions: filter },
                                 vec![prev_node.clone()],
                                 vec![]);
            new_nodes.push(n.clone());
            prev_node = n;
        }

        new_nodes
    }


    fn make_function_node(&mut self,
                          name: &str,
                          func_col: &Column,
                          group_cols: Vec<&Column>,
                          parent: MirNodeRef)
                          -> MirNodeRef {
        use ops::grouped::aggregate::Aggregation;
        use ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;

        let mknode = |over: &Column, t: GroupedNodeType| {
            self.make_grouped_node(name, &func_col, (parent, &over), group_cols, t)
        };

        let func = func_col.function.as_ref().unwrap();
        match *func.deref() {
            Sum(ref col, _) => mknode(col, GroupedNodeType::Aggregation(Aggregation::SUM)),
            Count(ref col, _) => mknode(col, GroupedNodeType::Aggregation(Aggregation::COUNT)),
            CountStar => {
                // XXX(malte): there is no "over" column, but our aggregation operators' API
                // requires one to be specified, so we earlier rewrote it to use the last parent
                // column (see passes/count_star_rewrite.rs). However, this isn't *entirely*
                // faithful to COUNT(*) semantics, because COUNT(*) is supposed to count all
                // rows including those with NULL values, and we don't have a mechanism to do that
                // (but we also don't have a NULL value, so maybe we're okay).
                panic!("COUNT(*) should have been rewritten earlier!")
            }
            Max(ref col) => mknode(col, GroupedNodeType::Extremum(Extremum::MAX)),
            Min(ref col) => mknode(col, GroupedNodeType::Extremum(Extremum::MIN)),
            GroupConcat(ref col, ref separator) => {
                mknode(col, GroupedNodeType::GroupConcat(separator.clone()))
            }
            _ => unimplemented!(),
        }
    }

    fn make_grouped_node(&mut self,
                         name: &str,
                         computed_col: &Column,
                         over: (MirNodeRef, &Column),
                         group_by: Vec<&Column>,
                         node_type: GroupedNodeType)
                         -> MirNodeRef {
        let parent_node = over.0;

        // Resolve column IDs in parent
        let over_col = over.1;

        // move alias to name in computed column (which needs not to
        // match against a parent node column, and is often aliased)
        let computed_col = match computed_col.alias {
            None => computed_col.clone(),
            Some(ref a) => {
                Column {
                    name: a.clone(),
                    alias: None,
                    table: computed_col.table.clone(),
                    function: computed_col.function.clone(),
                }
            }
        };

        // The function node's set of output columns is the group columns plus the function
        // column
        let mut combined_columns = group_by.iter().map(|c| (*c).clone()).collect::<Vec<Column>>();
        combined_columns.push(computed_col.clone());

        // make the new operator
        match node_type {
            GroupedNodeType::Aggregation(agg) => {
                MirNode::new(name,
                             self.schema_version,
                             combined_columns,
                             MirNodeType::Aggregation {
                                 on: over_col.clone(),
                                 group_by: group_by.into_iter().cloned().collect(),
                                 kind: agg,
                             },
                             vec![parent_node.clone()],
                             vec![])
            }
            GroupedNodeType::Extremum(extr) => {
                MirNode::new(name,
                             self.schema_version,
                             combined_columns,
                             MirNodeType::Extremum {
                                 on: over_col.clone(),
                                 group_by: group_by.into_iter().cloned().collect(),
                                 kind: extr,
                             },
                             vec![parent_node.clone()],
                             vec![])
            }
            GroupedNodeType::GroupConcat(sep) => {
                MirNode::new(name,
                             self.schema_version,
                             combined_columns,
                             MirNodeType::GroupConcat {
                                 on: over_col.clone(),
                                 separator: sep,
                             },
                             vec![parent_node.clone()],
                             vec![])
            }
        }
    }

    fn make_join_node(&mut self,
                      name: &str,
                      jps: &[ConditionTree],
                      left_node: MirNodeRef,
                      right_node: MirNodeRef,
                      kind: JoinType)
                      -> MirNodeRef {
        let projected_cols_left = left_node.borrow()
            .columns()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let projected_cols_right = right_node.borrow()
            .columns()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let fields = projected_cols_left.into_iter()
            .chain(projected_cols_right.into_iter())
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        // TODO(malte): no multi-level joins yet
        let mut left_join_columns = Vec::new();
        let mut right_join_columns = Vec::new();
        for p in jps.iter() {
            // equi-join only
            assert_eq!(p.operator, Operator::Equal);
            let l_col = match *p.left {
                ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                _ => unimplemented!(),
            };
            let r_col = match *p.right {
                ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                _ => unimplemented!(),
            };
            left_join_columns.push(l_col);
            right_join_columns.push(r_col);
        }
        assert_eq!(left_join_columns.len(), right_join_columns.len());
        let inner = match kind {
            JoinType::Inner => {
                MirNodeType::Join {
                    on_left: left_join_columns,
                    on_right: right_join_columns,
                    project: fields.clone(),
                }
            }
            JoinType::Left => {
                MirNodeType::LeftJoin {
                    on_left: left_join_columns,
                    on_right: right_join_columns,
                    project: fields.clone(),
                }
            }
        };
        MirNode::new(name,
                     self.schema_version,
                     fields,
                     inner,
                     vec![left_node.clone(), right_node.clone()],
                     vec![])
    }

    fn make_projection_helper(&mut self,
                              name: &str,
                              parent: MirNodeRef,
                              computed_col: &Column)
                              -> MirNodeRef {
        let fn_col = target_columns_from_computed_column(computed_col);

        self.make_project_node(name,
                               parent,
                               vec![fn_col],
                               vec![(String::from("grp"), DataType::from(0 as i32))])
    }

    fn make_project_node(&mut self,
                         name: &str,
                         parent_node: MirNodeRef,
                         proj_cols: Vec<&Column>,
                         literals: Vec<(String, DataType)>)
                         -> MirNodeRef {
        //assert!(proj_cols.iter().all(|c| c.table == parent_name));

        let literal_names: Vec<String> = literals.iter().map(|&(ref n, _)| n.clone()).collect();
        let fields = proj_cols.clone()
            .into_iter()
            .map(|c| match c.alias {
                     Some(ref a) => {
                         Column {
                             name: a.clone(),
                             table: c.table.clone(),
                             alias: Some(a.clone()),
                             function: c.function.clone(),
                         }
                     }
                     None => c.clone(),
                 })
            .chain(literal_names.into_iter().map(|n| {
                Column {
                    name: n,
                    alias: None,
                    table: Some(String::from(name)),
                    function: None,
                }
            }))
            .collect();

        // remove aliases from emit columns because they are later compared to parent node columns
        // and need to be equal. Note that `fields`, which holds the column names applied,
        // preserves the aliases.
        let emit_cols = proj_cols.into_iter()
            .cloned()
            .map(|mut c| {
                match c.alias {
                    Some(_) => c.alias = None,
                    None => (),
                };
                c
            })
            .collect();

        MirNode::new(name,
                     self.schema_version,
                     fields,
                     MirNodeType::Project {
                         emit: emit_cols,
                         literals: literals,
                     },
                     vec![parent_node.clone()],
                     vec![])
    }

    fn make_topk_node(&mut self,
                      name: &str,
                      parent: MirNodeRef,
                      group_by: Vec<&Column>,
                      order: &Option<OrderClause>,
                      limit: &LimitClause)
                      -> MirNodeRef {
        let combined_columns = parent.borrow()
            .columns()
            .iter()
            .cloned()
            .collect();

        let order = match *order {
            Some(ref o) => Some(o.columns.clone()),
            None => None,
        };

        assert_eq!(limit.offset, 0); // Non-zero offset not supported

        // make the new operator and record its metadata
        MirNode::new(name,
                     self.schema_version,
                     combined_columns,
                     MirNodeType::TopK {
                         order: order,
                         group_by: group_by.into_iter().cloned().collect(),
                         k: limit.limit as usize,
                         offset: 0,
                     },
                     vec![parent.clone()],
                     vec![])
    }

    /// Returns list of nodes added
    fn make_nodes_for_selection(&mut self,
                                name: &str,
                                st: &SelectStatement,
                                qg: &QueryGraph)
                                -> Vec<MirNodeRef> {
        use std::collections::HashMap;

        let mut nodes_added: Vec<MirNodeRef>;
        let mut new_node_count = 0;

        // Canonical operator order: B-J-G-F-P-R
        // (Base, Join, GroupBy, Filter, Project, Reader)
        {
            // 0. Base nodes (always reused)
            let mut base_nodes: HashMap<&str, MirNodeRef> = HashMap::default();
            let mut sorted_rels: Vec<&str> = qg.relations
                .keys()
                .map(String::as_str)
                .collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                // the node holding computed columns doesn't have a base
                if *rel == "computed_columns" {
                    continue;
                }

                let latest_existing = self.current.get(*rel);
                let base_for_rel = match latest_existing {
                    None => panic!("Query \"{}\" refers to unknown base node \"{}\"", name, rel),
                    Some(v) => {
                        let existing = self.nodes.get(&(String::from(*rel), *v));
                        match existing {
                            None => {
                                panic!("Inconsistency: base node \"{}\" does not exist at v{}",
                                       *rel,
                                       v);
                            }
                            Some(bmn) => MirNode::reuse(bmn.clone(), self.schema_version),
                        }
                    }
                };
                base_nodes.insert(*rel, base_for_rel);
            }

            // 1. Generate join nodes for the query. This starts out by joining two of the base
            //    nodes corresponding to relations in the first join predicate, and then continues
            //    to join the result against previously unseen tables from the remaining
            //    predicates. Note that no (src, dst) pair ever occurs twice, since we've already
            //    previously moved all predicates pertaining to src/dst joins onto a single edge.
            let mut join_nodes: Vec<MirNodeRef> = Vec::new();
            let mut joined_tables = HashSet::new();
            let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> =
                qg.edges.iter().collect();
            sorted_edges.sort_by_key(|k| &(k.0).0);
            let mut prev_node = None;

            {
                let pick_join_columns = |src: &String,
                                         dst: &String,
                                         prev_node: Option<MirNodeRef>,
                                         joined_tables: &HashSet<_>|
                 -> (MirNodeRef, MirNodeRef) {
                    let left_node;
                    let right_node;
                    if joined_tables.contains(src) && joined_tables.contains(dst) {
                        // We have already handled *both* tables that are part of the join.
                        // This should never occur, because their join predicates must be
                        // associated with the same query graph edge.
                        unreachable!();
                    } else if joined_tables.contains(src) {
                        // join left against previous join, right against base
                        left_node = prev_node.as_ref().unwrap().clone();
                        right_node = base_nodes[dst.as_str()].clone();
                    } else if joined_tables.contains(dst) {
                        // join right against previous join, left against base
                        left_node = base_nodes[src.as_str()].clone();
                        right_node = prev_node.as_ref().unwrap().clone();
                    } else {
                        // We've seen neither of these tables before
                        // If we already have a join in prev_ni, we must assume that some
                        // future join will bring these unrelated join arms together.
                        // TODO(malte): make that actually work out...
                        left_node = base_nodes[src.as_str()].clone();
                        right_node = base_nodes[dst.as_str()].clone();
                    }
                    (left_node, right_node)
                };

                for &(&(ref src, ref dst), edge) in &sorted_edges {
                    let jn = match *edge {
                        // Edge represents a LEFT JOIN
                        QueryGraphEdge::LeftJoin(ref jps) => {
                            let (left_node, right_node) =
                                pick_join_columns(src, dst, prev_node, &joined_tables);
                            self.make_join_node(&format!("q_{:x}_n{}",
                                                         qg.signature().hash,
                                                         new_node_count),
                                                jps,
                                                left_node,
                                                right_node,
                                                JoinType::Left)
                        }
                        // Edge represents a JOIN
                        QueryGraphEdge::Join(ref jps) => {
                            let (left_node, right_node) =
                                pick_join_columns(src, dst, prev_node, &joined_tables);
                            self.make_join_node(&format!("q_{:x}_n{}",
                                                         qg.signature().hash,
                                                         new_node_count),
                                                jps,
                                                left_node,
                                                right_node,
                                                JoinType::Inner)
                        }
                        // Edge represents a GROUP BY, which we handle later
                        QueryGraphEdge::GroupBy(_) => continue,
                    };

                    // bookkeeping (shared between both join types)
                    join_nodes.push(jn.clone());
                    new_node_count += 1;
                    prev_node = Some(jn);

                    // we've now joined both tables
                    joined_tables.insert(src);
                    joined_tables.insert(dst);
                }
            }

            // 3. Grouped and function nodes
            let mut func_nodes: Vec<MirNodeRef> = Vec::new();
            match qg.relations.get("computed_columns") {
                None => (),
                Some(computed_cols_cgn) => {
                    // Function columns with GROUP BY clause
                    let mut grouped_fn_columns = HashSet::new();
                    for e in qg.edges.values() {
                        match *e {
                            QueryGraphEdge::Join(_) |
                            QueryGraphEdge::LeftJoin(_) => (),
                            QueryGraphEdge::GroupBy(ref gb_cols) => {
                                // Generate the right function nodes for all relevant columns in
                                // the "computed_columns" node

                                // TODO(malte): there can only be one GROUP BY in each query, but
                                // the columns can come from different tables. In that case, we
                                // would need to generate an Agg-Join-Agg sequence for each pair of
                                // tables involved.
                                let table = gb_cols.iter()
                                    .next()
                                    .unwrap()
                                    .table
                                    .as_ref()
                                    .unwrap();
                                assert!(gb_cols.iter().all(|c| c.table.as_ref().unwrap() == table));

                                for fn_col in &computed_cols_cgn.columns {
                                    // we must also push parameter columns through the group by
                                    let over_col = target_columns_from_computed_column(fn_col);
                                    let over_table = over_col.table
                                        .as_ref()
                                        .unwrap()
                                        .as_str();
                                    // get any parameter columns that aren't also in the group-by
                                    // column set
                                    let param_cols: Vec<_> = qg.relations
                                        .get(over_table)
                                        .as_ref()
                                        .unwrap()
                                        .parameters
                                        .iter()
                                        .filter(|ref c| !gb_cols.contains(c))
                                        .collect();
                                    // combine
                                    let gb_and_param_cols: Vec<_> =
                                        gb_cols.iter().chain(param_cols.into_iter()).collect();


                                    let parent_node = match prev_node {
                                        // If no explicit parent node is specified, we extract
                                        // the base node from the "over" column's specification
                                        None => base_nodes[over_table].clone(),
                                        // We have an explicit parent node (likely a projection helper), so use that
                                        Some(node) => node,
                                    };

                                    let n = self.make_function_node(&format!("q_{:x}_n{}",
                                                                             qg.signature().hash,
                                                                             new_node_count),
                                                                    fn_col,
                                                                    gb_and_param_cols,
                                                                    parent_node);
                                    prev_node = Some(n.clone());
                                    func_nodes.push(n);
                                    grouped_fn_columns.insert(fn_col);
                                    new_node_count += 1;
                                }
                            }
                        }
                    }
                    // Function columns without GROUP BY
                    for computed_col in computed_cols_cgn.columns
                            .iter()
                            .filter(|c| !grouped_fn_columns.contains(c))
                            .collect::<Vec<_>>() {

                        let agg_node_name =
                            &format!("q_{:x}_n{}", qg.signature().hash, new_node_count);

                        let over_col = target_columns_from_computed_column(computed_col);
                        let over_table = over_col.table
                            .as_ref()
                            .unwrap()
                            .as_str();

                        let ref proj_cols_from_target_table = qg.relations
                            .get(over_table)
                            .as_ref()
                            .unwrap()
                            .columns;

                        let parent_node = match prev_node {
                            Some(ref node) => node.clone(),
                            None => base_nodes[over_table].clone(),
                        };

                        let (group_cols, parent_node) = if proj_cols_from_target_table.is_empty() {
                            // slightly messy hack: if there are no group columns and the table on
                            // which we compute has no projected columns in the output, we make one
                            // up a group column by adding an extra projection node
                            let proj_name = format!("{}_prj_hlpr", agg_node_name);
                            let proj =
                                self.make_projection_helper(&proj_name, parent_node, computed_col);

                            func_nodes.push(proj.clone());
                            new_node_count += 1;

                            let bogo_group_col = Column::from(format!("{}.grp", proj_name)
                                                                  .as_str());
                            (vec![bogo_group_col], proj)
                        } else {
                            (proj_cols_from_target_table.clone(), parent_node)
                        };
                        let n = self.make_function_node(agg_node_name,
                                                        computed_col,
                                                        group_cols.iter().collect(),
                                                        parent_node);
                        prev_node = Some(n.clone());
                        func_nodes.push(n);
                        new_node_count += 1;
                    }
                }
            }

            // 3. Generate the necessary filter node for each relation node in the query graph.
            let mut filter_nodes = Vec::new();
            // Need to iterate over relations in a deterministic order, as otherwise nodes will be
            // added in a different order every time, which will yield different node identifiers
            // and make it difficult for applications to check what's going on.
            let mut sorted_rels: Vec<&String> = qg.relations.keys().collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                let qgn = &qg.relations[*rel];
                // we've already handled computed columns
                if *rel != "computed_columns" {
                    // the following conditional is required to avoid "empty" nodes (without any
                    // projected columns) that are required as inputs to joins
                    if !qgn.predicates.is_empty() {
                        // add a filter chain for each query graph node's predicates
                        let parent = match prev_node {
                            None => base_nodes[rel.as_str()].clone(),
                            Some(pn) => pn,
                        };
                        let fns = self.make_filter_nodes(&format!("q_{:x}_n{}",
                                                                  qg.signature().hash,
                                                                  new_node_count),
                                                         parent,
                                                         &qgn.predicates);
                        assert!(fns.len() > 0);
                        new_node_count += fns.len();
                        prev_node = Some(fns.iter()
                                             .last()
                                             .unwrap()
                                             .clone());
                        filter_nodes.extend(fns);
                    }
                }
            }

            // 4. Get the final node
            let mut final_node: MirNodeRef = if !filter_nodes.is_empty() {
                filter_nodes.last().unwrap().clone()
            } else if !func_nodes.is_empty() {
                // TODO(malte): This won't work if computed columns are used within JOIN clauses
                func_nodes.last().unwrap().clone()
            } else if !join_nodes.is_empty() {
                join_nodes.last().unwrap().clone()
            } else {
                // no join, filter, or function node --> base node is parent
                assert_eq!(sorted_rels.len(), 1);
                base_nodes[sorted_rels.last().unwrap().as_str()].clone()
            };

            // 4. Potentially insert TopK node below the final node
            if let Some(ref limit) = st.limit {
                let group_by = qg.parameters();

                let node = self.make_topk_node(&format!("q_{:x}_n{}",
                                                        qg.signature().hash,
                                                        new_node_count),
                                               final_node,
                                               group_by,
                                               &st.order,
                                               limit);
                func_nodes.push(node.clone());
                final_node = node;
                new_node_count += 1;
            }

            // should have counted all nodes added, except for the base nodes (which reuse)
            debug_assert_eq!(new_node_count,
                             join_nodes.len() + func_nodes.len() + filter_nodes.len());
            // we're now done with the query, so remember all the nodes we've added so far
            nodes_added = base_nodes.into_iter()
                .map(|(_, n)| n)
                .chain(join_nodes.into_iter())
                .chain(func_nodes.into_iter())
                .chain(filter_nodes.into_iter())
                .collect();

            // 5. Generate leaf views that expose the query result
            let projected_columns: Vec<&Column> = sorted_rels.iter().fold(Vec::new(), |mut v, s| {
                v.extend(qg.relations[*s].columns.iter());
                v
            });

            // translate aliases on leaf columns only
            let leaf_project_node =
                self.make_project_node(&format!("q_{:x}_n{}", qg.signature().hash, new_node_count),
                                       final_node,
                                       projected_columns,
                                       vec![]);
            nodes_added.push(leaf_project_node.clone());

            // We always materialize leaves of queries (at least currently), so add a
            // `MaterializedLeaf` node keyed on the query parameters.
            let query_params = qg.parameters();
            let columns = leaf_project_node.borrow()
                .columns()
                .iter()
                .cloned()
                .collect();

            let leaf_node = MirNode::new(name,
                                         self.schema_version,
                                         columns,
                                         MirNodeType::Leaf {
                                             node: leaf_project_node.clone(),
                                             keys: query_params.into_iter().cloned().collect(),
                                         },
                                         vec![leaf_project_node.clone()],
                                         vec![]);
            nodes_added.push(leaf_node);

            debug!(self.log,
                   format!("Added final MIR node for query named \"{}\"", name));
        }

        // finally, we output all the nodes we generated
        nodes_added
    }
}