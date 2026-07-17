use super::*;

#[test]
fn point_struct_x_y() {
    assert_eq!(parse_point("(1,2)").unwrap(), Pt { x: 1.0, y: 2.0 });
    let f = geometric_field("p", oids::POINT).unwrap();
    assert_eq!(f.data_type(), &point_struct());
}

#[test]
fn box_is_two_nested_points() {
    assert_eq!(
        parse_box("(2,3),(0,1)").unwrap(),
        (Pt { x: 2.0, y: 3.0 }, Pt { x: 0.0, y: 1.0 })
    );
    // lseg uses the same two-point shape, with brackets.
    assert_eq!(
        parse_box("[(0,0),(1,1)]").unwrap(),
        (Pt { x: 0.0, y: 0.0 }, Pt { x: 1.0, y: 1.0 })
    );
}

#[test]
fn path_open_vs_closed_sets_is_closed() {
    // Same points, different delimiters → the ONLY difference is is_closed.
    let (open, open_pts) = parse_path("[(0,0),(1,1)]").unwrap();
    let (closed, closed_pts) = parse_path("((0,0),(1,1))").unwrap();
    assert!(!open, "brackets → open path");
    assert!(closed, "double parens → closed path");
    assert_eq!(open_pts, closed_pts);
}

#[test]
fn polygon_is_list_of_points() {
    let pts = parse_polygon("((0,0),(1,0),(1,1))").unwrap();
    assert_eq!(pts.len(), 3);
    assert_eq!(pts[2], Pt { x: 1.0, y: 1.0 });
}

#[test]
fn circle_carries_radius() {
    assert_eq!(
        parse_circle("<(1,2),3>").unwrap(),
        (Pt { x: 1.0, y: 2.0 }, 3.0)
    );
}

#[test]
fn line_is_three_coefficients() {
    assert_eq!(parse_line("{1,2,3}").unwrap(), (1.0, 2.0, 3.0));
}

#[test]
fn postgis_and_unknown_oids_are_not_geometric() {
    // A PostGIS geometry OID is install-specific and never matches — deferred by design.
    assert_eq!(geometric_field("g", 99999), None);
    assert_eq!(geo_kind(99999), None);
}
