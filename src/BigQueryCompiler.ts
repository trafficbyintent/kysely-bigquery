import {
  type ColumnDefinitionNode,
  type DeleteQueryNode,
  type ForeignKeyConstraintNode,
  type FunctionNode,
  type IdentifierNode,
  MysqlQueryCompiler,
  type OperationNode,
  type PrimaryKeyConstraintNode,
  type RawNode,
  type SetOperationNode,
  type TableNode,
  type UniqueConstraintNode,
  type UpdateQueryNode,
} from 'kysely';

/**
 * Query compiler for BigQuery dialect.
 *
 * Extends MysqlQueryCompiler and overrides methods to generate
 * BigQuery-compatible SQL.
 */
export class BigQueryCompiler extends MysqlQueryCompiler {
  readonly #defaultProject: string | undefined;

  constructor(defaultProject?: string) {
    super();
    this.#defaultProject = defaultProject;
  }

  protected override visitSetOperation(node: SetOperationNode): void {
    if (node.operator === 'union' && !node.all) {
      this.append('union distinct ');
      this.visitNode(node.expression);
    } else {
      super.visitSetOperation(node);
    }
  }

  protected override visitFunction(node: FunctionNode): void {
    const funcName = node.func.toUpperCase();

    switch (funcName) {
      case 'NOW':
        this.append('CURRENT_TIMESTAMP');
        this.visitFunctionArgumentList(node.arguments);
        return;

      case 'LENGTH':
        /* BigQuery uses CHAR_LENGTH for character count */
        this.append('CHAR_LENGTH');
        this.visitFunctionArgumentList(node.arguments);
        return;

      case 'DATE_FORMAT':
        if (
          node.arguments.length === 2 &&
          node.arguments[0] !== null &&
          node.arguments[0] !== undefined &&
          node.arguments[1] !== null &&
          node.arguments[1] !== undefined
        ) {
          this.append('FORMAT_TIMESTAMP');
          this.append('(');
          this.visitNode(node.arguments[1]);
          this.append(', ');
          this.visitNode(node.arguments[0]);
          this.append(')');
          return;
        }
        break;

      case 'DATE_ADD':
        /*
         * Pass through DATE_ADD as BigQuery supports it
         * Note: BigQuery syntax is DATE_ADD(date, INTERVAL n unit)
         */
        break;

      default:
        /* Use default handler for all other functions */
        break;
    }

    super.visitFunction(node);
  }

  protected override visitUpdateQuery(node: UpdateQueryNode): void {
    if (node.where === null || node.where === undefined) {
      /*
       * BigQuery requires WHERE clause for UPDATE statements
       * Add WHERE TRUE to allow the query to execute
       */
      super.visitUpdateQuery(node);
      this.append(' where true');
      return;
    }
    super.visitUpdateQuery(node);
  }

  protected override visitDeleteQuery(node: DeleteQueryNode): void {
    if (node.where === null || node.where === undefined) {
      /*
       * BigQuery requires WHERE clause for DELETE statements
       * Add WHERE TRUE to allow the query to execute
       */
      super.visitDeleteQuery(node);
      this.append(' where true');
      return;
    }
    super.visitDeleteQuery(node);
  }

  protected override visitTable(node: TableNode): void {
    if (this.#defaultProject && node.table.kind === 'SchemableIdentifierNode') {
      const schemableNode = node.table;

      if (schemableNode.schema && schemableNode.schema.kind === 'IdentifierNode') {
        this.visitIdentifier({
          kind: 'IdentifierNode',
          name: this.#defaultProject,
        } as IdentifierNode);
        this.append('.');
        this.visitIdentifier(schemableNode.schema);
        this.append('.');
        this.visitIdentifier(schemableNode.identifier);
        return;
      }
    }

    super.visitTable(node);
  }

  protected override visitRaw(node: RawNode): void {
    const translatedFragments = [...node.sqlFragments];

    for (let i = 0; i < translatedFragments.length; i++) {
      let fragment = translatedFragments[i];
      if (fragment === null || fragment === undefined || fragment === '') {
        continue;
      }

      fragment = fragment.replace(/\bNOW\s*\(\s*\)/gi, 'CURRENT_TIMESTAMP()');

      translatedFragments[i] = fragment;
    }

    if (translatedFragments.some((f) => f.match(/\bDATE_FORMAT\s*\(/i))) {
      for (let i = 0; i < translatedFragments.length; i++) {
        let fragment = translatedFragments[i];
        /* c8 ignore start - defensive: fragments already filtered above */
        if (fragment === null || fragment === undefined || fragment === '') {
          continue;
        }
        /* c8 ignore stop */

        const fullMatch = fragment.match(/\bDATE_FORMAT\s*\(\s*([^,]+)\s*,\s*('[^']*')\s*\)/gi);
        if (fullMatch) {
          fragment = fragment.replace(
            /\bDATE_FORMAT\s*\(\s*([^,]+)\s*,\s*('[^']*')\s*\)/gi,
            'FORMAT_TIMESTAMP($2, $1)',
          );
          translatedFragments[i] = fragment;
        } else {
          translatedFragments[i] = fragment.replace(/\bDATE_FORMAT\s*\(/gi, 'FORMAT_TIMESTAMP(');
        }
      }

      if (translatedFragments.length === 2 && node.parameters.length === 1) {
        const secondFragment = translatedFragments[1];
        if (secondFragment) {
          const formatMatch = secondFragment.match(/,\s*'([^']*)'(.*)$/);
          if (formatMatch && translatedFragments[0] && node.parameters[0]) {
            this.append(translatedFragments[0]);
            this.append(`'${formatMatch[1]}'`);
            this.append(', ');
            this.visitNode(node.parameters[0]);
            this.append(formatMatch[2] || '');
            return;
          }
        }
      }

      this.appendFragmentsWithParams(translatedFragments, node.parameters);
    } else {
      this.appendFragmentsWithParams(translatedFragments, node.parameters);
    }
  }

  private appendFragmentsWithParams(
    fragments: string[],
    params: ReadonlyArray<OperationNode>,
  ): void {
    for (let i = 0; i < fragments.length; i++) {
      if (i > 0) {
        const param = params[i - 1];
        if (param) {
          this.visitNode(param);
        }
      }
      const fragment = fragments[i];
      if (fragment) {
        this.append(fragment);
      }
    }
  }

  protected visitFunctionArgumentList(args: ReadonlyArray<OperationNode>): void {
    this.append('(');
    const lastNode = args[args.length - 1];

    for (const node of args) {
      this.visitNode(node);

      /* c8 ignore start - defensive: currently only called with 0-1 args */
      if (node !== lastNode) {
        this.append(', ');
      }
      /* c8 ignore stop */
    }
    this.append(')');
  }

  protected override visitColumnDefinition(node: ColumnDefinitionNode): void {
    /* Call parent implementation first */
    super.visitColumnDefinition(node);

    /* If column has inline constraints, append NOT ENFORCED */
    if (node.primaryKey || node.unique || node.references) {
      this.append(' not enforced');
    }
  }

  protected override visitPrimaryKeyConstraint(node: PrimaryKeyConstraintNode): void {
    if (node.name) {
      this.append('constraint ');
      this.visitNode(node.name);
      this.append(' ');
    }
    this.append('primary key');
    if (node.columns) {
      this.append(' (');
      this.compileList(node.columns);
      this.append(')');
    }
    /* BigQuery requires NOT ENFORCED for all constraints */
    this.append(' not enforced');
  }

  protected override visitUniqueConstraint(node: UniqueConstraintNode): void {
    if (node.name) {
      this.append('constraint ');
      this.visitNode(node.name);
      this.append(' ');
    }
    this.append('unique');
    if (node.columns) {
      this.append(' (');
      this.compileList(node.columns);
      this.append(')');
    }
    /* BigQuery requires NOT ENFORCED for all constraints */
    this.append(' not enforced');
  }

  protected override visitForeignKeyConstraint(node: ForeignKeyConstraintNode): void {
    if (node.name) {
      this.append('constraint ');
      this.visitNode(node.name);
      this.append(' ');
    }
    this.append('foreign key (');
    this.compileList(node.columns);
    this.append(') ');
    this.visitNode(node.references);
    if (node.onDelete) {
      this.append(' on delete ');
      this.append(node.onDelete);
    }
    if (node.onUpdate) {
      this.append(' on update ');
      this.append(node.onUpdate);
    }
    /* BigQuery requires NOT ENFORCED for all constraints */
    this.append(' not enforced');
  }
}
