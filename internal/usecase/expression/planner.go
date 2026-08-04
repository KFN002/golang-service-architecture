package expression

import (
	"github.com/google/uuid"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/constants"
)

// Plan flattens an AST into a task DAG for the given expression.
//
// Leaves collapse into their parent operation's literal arguments; inner
// operations depend on child task results. If the whole expression is a single
// literal (e.g. "42"), no tasks are produced and the value is returned via
// immediate — the caller finalizes the expression synchronously.
func Plan(exprID uuid.UUID, root *Node) (tasks []*entity.Task, immediate *float64, err error) {
	if root.IsLeaf() {
		v := root.Value
		return nil, &v, nil
	}

	var build func(n *Node) (*entity.Task, error)
	build = func(n *Node) (*entity.Task, error) {
		t := &entity.Task{
			ID:           uuid.New(),
			ExpressionID: exprID,
			Op:           n.Op,
			Status:       entity.TaskPending,
		}

		if n.Left.IsLeaf() {
			v := n.Left.Value
			t.Arg1Value = &v
		} else {
			child, err := build(n.Left)
			if err != nil {
				return nil, err
			}
			t.Arg1TaskID = &child.ID
			t.UnmetDeps++
		}

		if n.Right.IsLeaf() {
			v := n.Right.Value
			t.Arg2Value = &v
		} else {
			child, err := build(n.Right)
			if err != nil {
				return nil, err
			}
			t.Arg2TaskID = &child.ID
			t.UnmetDeps++
		}

		tasks = append(tasks, t)
		// Post-order append is the only place the count grows, so the limit
		// check belongs here — an entry check never fires during the initial
		// depth-first descent.
		if len(tasks) > constants.MaxTasksPerExpr {
			return nil, apperrors.Newf(apperrors.CodeInvalidInput,
				"expression produces more than %d operations", constants.MaxTasksPerExpr)
		}
		return t, nil
	}

	rootTask, err := build(root)
	if err != nil {
		return nil, nil, err
	}
	rootTask.IsRoot = true
	return tasks, nil, nil
}
