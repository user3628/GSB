"""Deterministic polygon geometry used by decode and IoU matching."""

from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np

from .canonical import quantize

Point = tuple[float, float]

EPS = 1e-9


def clean_vertices(vertices: Sequence[Point], eps: float = EPS) -> list[Point]:
    points: list[Point] = []
    for point in vertices:
        current = (float(point[0]), float(point[1]))
        if not points or abs(points[-1][0] - current[0]) > eps or abs(points[-1][1] - current[1]) > eps:
            points.append(current)
    if len(points) > 1:
        first = points[0]
        last = points[-1]
        if abs(first[0] - last[0]) <= eps and abs(first[1] - last[1]) <= eps:
            points.pop()
    return points


def signed_area(vertices: Sequence[Point]) -> float:
    points = clean_vertices(vertices)
    if len(points) < 3:
        return 0.0
    array = np.asarray(points, dtype=np.float64)
    xs = array[:, 0]
    ys = array[:, 1]
    total = np.dot(xs, np.roll(ys, -1)) - np.dot(ys, np.roll(xs, -1))
    return float(total) / 2.0


def polygon_area(vertices: Sequence[Point]) -> float:
    return abs(signed_area(vertices))


def polygon_bbox(vertices: Sequence[Point]) -> list[float] | None:
    points = clean_vertices(vertices)
    if not points:
        return None
    array = np.asarray(points, dtype=np.float64)
    return [
        float(np.min(array[:, 0])),
        float(np.min(array[:, 1])),
        float(np.max(array[:, 0])),
        float(np.max(array[:, 1])),
    ]


def _cross(a: Point, b: Point, c: Point) -> float:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _on_segment(a: Point, b: Point, point: Point, eps: float = EPS) -> bool:
    if abs(_cross(a, b, point)) > eps:
        return False
    return (
        min(a[0], b[0]) - eps <= point[0] <= max(a[0], b[0]) + eps
        and min(a[1], b[1]) - eps <= point[1] <= max(a[1], b[1]) + eps
    )


def segments_intersect(a: Point, b: Point, c: Point, d: Point, eps: float = EPS) -> bool:
    o1 = _cross(a, b, c)
    o2 = _cross(a, b, d)
    o3 = _cross(c, d, a)
    o4 = _cross(c, d, b)
    if ((o1 > eps and o2 < -eps) or (o1 < -eps and o2 > eps)) and (
        (o3 > eps and o4 < -eps) or (o3 < -eps and o4 > eps)
    ):
        return True
    if abs(o1) <= eps and _on_segment(a, b, c, eps):
        return True
    if abs(o2) <= eps and _on_segment(a, b, d, eps):
        return True
    if abs(o3) <= eps and _on_segment(c, d, a, eps):
        return True
    if abs(o4) <= eps and _on_segment(c, d, b, eps):
        return True
    return False


def has_self_intersection(vertices: Sequence[Point]) -> bool:
    points = clean_vertices(vertices)
    count = len(points)
    if count < 4:
        return False
    for left in range(count):
        a = points[left]
        b = points[(left + 1) % count]
        if abs(a[0] - b[0]) <= EPS and abs(a[1] - b[1]) <= EPS:
            continue
        for right in range(left + 1, count):
            if right == left or right == (left + 1) % count or (right + 1) % count == left:
                continue
            c = points[right]
            d = points[(right + 1) % count]
            if abs(c[0] - d[0]) <= EPS and abs(c[1] - d[1]) <= EPS:
                continue
            if segments_intersect(a, b, c, d):
                return True
    return False


def _point_in_triangle(point: Point, a: Point, b: Point, c: Point, eps: float = EPS) -> bool:
    c1 = _cross(a, b, point)
    c2 = _cross(b, c, point)
    c3 = _cross(c, a, point)
    has_neg = c1 < -eps or c2 < -eps or c3 < -eps
    has_pos = c1 > eps or c2 > eps or c3 > eps
    return not (has_neg and has_pos)


def triangulate(vertices: Sequence[Point]) -> list[tuple[Point, Point, Point]]:
    points = clean_vertices(vertices)
    if len(points) < 3 or polygon_area(points) <= EPS:
        return []
    if signed_area(points) < 0:
        points.reverse()
    indices = list(range(len(points)))
    triangles: list[tuple[Point, Point, Point]] = []
    guard = 0
    while len(indices) > 3:
        guard += 1
        if guard > len(points) * len(points) + 10:
            break
        ear_found = False
        for cursor in range(len(indices)):
            previous = indices[(cursor - 1) % len(indices)]
            current = indices[cursor]
            following = indices[(cursor + 1) % len(indices)]
            a, b, c = points[previous], points[current], points[following]
            if _cross(a, b, c) <= EPS:
                continue
            other_indices = [idx for idx in indices if idx not in {previous, current, following}]
            if any(_point_in_triangle(points[idx], a, b, c) for idx in other_indices):
                continue
            triangles.append((a, b, c))
            del indices[cursor]
            ear_found = True
            break
        if ear_found:
            continue
        removed_collinear = False
        for cursor in range(len(indices)):
            previous = indices[(cursor - 1) % len(indices)]
            current = indices[cursor]
            following = indices[(cursor + 1) % len(indices)]
            if abs(_cross(points[previous], points[current], points[following])) <= EPS:
                del indices[cursor]
                removed_collinear = True
                break
        if not removed_collinear:
            break
    if len(indices) == 3:
        triangles.append((points[indices[0]], points[indices[1]], points[indices[2]]))
    return triangles


def _line_intersection(p1: Point, p2: Point, q1: Point, q2: Point) -> Point:
    dx1 = p2[0] - p1[0]
    dy1 = p2[1] - p1[1]
    dx2 = q2[0] - q1[0]
    dy2 = q2[1] - q1[1]
    denominator = dx1 * dy2 - dy1 * dx2
    if abs(denominator) <= EPS:
        return p2
    t = ((q1[0] - p1[0]) * dy2 - (q1[1] - p1[1]) * dx2) / denominator
    return (p1[0] + t * dx1, p1[1] + t * dy1)


def _clip_convex(subject: Sequence[Point], clip: Sequence[Point]) -> list[Point]:
    output = list(subject)
    if len(output) < 3:
        return []
    clip_points = list(clip)
    if signed_area(clip_points) < 0:
        clip_points.reverse()
    for index, edge_start in enumerate(clip_points):
        edge_end = clip_points[(index + 1) % len(clip_points)]
        input_points = output
        output = []
        if not input_points:
            break
        previous = input_points[-1]
        previous_inside = _cross(edge_start, edge_end, previous) >= -EPS
        for current in input_points:
            current_inside = _cross(edge_start, edge_end, current) >= -EPS
            if current_inside:
                if not previous_inside:
                    output.append(_line_intersection(previous, current, edge_start, edge_end))
                output.append(current)
            elif previous_inside:
                output.append(_line_intersection(previous, current, edge_start, edge_end))
            previous = current
            previous_inside = current_inside
    return output


def _bboxes_disjoint(a: Sequence[Point], b: Sequence[Point]) -> bool:
    bbox_a = polygon_bbox(a)
    bbox_b = polygon_bbox(b)
    if bbox_a is None or bbox_b is None:
        return True
    return (
        bbox_a[2] < bbox_b[0] - EPS
        or bbox_b[2] < bbox_a[0] - EPS
        or bbox_a[3] < bbox_b[1] - EPS
        or bbox_b[3] < bbox_a[1] - EPS
    )


def polygon_intersection_area(a: Sequence[Point], b: Sequence[Point]) -> float:
    if _bboxes_disjoint(a, b):
        return 0.0
    triangles_a = triangulate(a)
    triangles_b = triangulate(b)
    if not triangles_a or not triangles_b:
        return 0.0
    total = 0.0
    for tri_a in triangles_a:
        for tri_b in triangles_b:
            if _bboxes_disjoint(tri_a, tri_b):
                continue
            clipped = _clip_convex(tri_a, tri_b)
            if len(clipped) >= 3:
                total += polygon_area(clipped)
    return total


def polygon_iou(a: Sequence[Point], b: Sequence[Point], area_a: float | None = None, area_b: float | None = None) -> float:
    if area_a is None:
        area_a = polygon_area(a)
    if area_b is None:
        area_b = polygon_area(b)
    intersection = polygon_intersection_area(a, b)
    union = area_a + area_b - intersection
    if union <= EPS:
        return 0.0
    return max(0.0, min(1.0, intersection / union))


def canonical_points(points: Iterable[Point], places: int = 9) -> list[list[float]]:
    return [[quantize(float(x), places), quantize(float(y), places)] for x, y in points]

