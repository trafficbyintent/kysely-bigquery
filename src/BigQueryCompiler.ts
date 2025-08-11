import {
  type ColumnDefinitionNode,
  type CreateTableNode,
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
  type ValueNode,
} from 'kysely';

/**
 * Query compiler for BigQuery dialect.
 *
 * Extends MysqlQueryCompiler and overrides methods to generate
 * BigQuery-compatible SQL.
 */
export class BigQueryCompiler extends MysqlQueryCompiler {
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
      /* c8 ignore start - NOW function translation */
      case 'NOW':
        this.append('CURRENT_TIMESTAMP');
        this.visitFunctionArgumentList(node.arguments);
        return;
      /* c8 ignore stop */

      case 'LENGTH':
        /* BigQuery uses CHAR_LENGTH for character count */
        this.append('CHAR_LENGTH');
        this.visitFunctionArgumentList(node.arguments);
        return;

      /* c8 ignore start - DATE_FORMAT function translation */
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
          this.visitNode(node.arguments[1]); // format first
          this.append(', ');
          this.visitNode(node.arguments[0]); // then date
          this.append(')');
          return;
        }
        break;
      /* c8 ignore stop */

      /* c8 ignore start - DATE_ADD passthrough */
      case 'DATE_ADD':
        /*
         * Pass through DATE_ADD as BigQuery supports it
         * Note: BigQuery syntax is DATE_ADD(date, INTERVAL n unit)
         */
        break;
      /* c8 ignore stop */

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

  protected override visitCreateTable(node: CreateTableNode): void {
    super.visitCreateTable(node);
  }

  protected override visitTable(node: TableNode): void {
    const { table } = node;

    if (table.kind === 'SchemableIdentifierNode') {
      const schemableNode = table;

      if (schemableNode.schema && schemableNode.schema.kind === 'IdentifierNode') {
        const schemaName = schemableNode.schema.name;

        /* c8 ignore start - project.dataset.table parsing for BigQuery */
        if (schemaName.includes('.')) {
          const parts = schemaName.split('.');
          if (parts.length === 2) {
            const [project, dataset] = parts;
            this.visitIdentifier({ kind: 'IdentifierNode', name: project } as IdentifierNode);
            this.append('.');
            this.visitIdentifier({ kind: 'IdentifierNode', name: dataset } as IdentifierNode);
            this.append('.');
            this.visitIdentifier(schemableNode.identifier);
            return;
          }
          /* If not exactly 2 parts, fall through to default behavior */
        }
        /* c8 ignore stop */
      }
    }

    super.visitTable(node);
  }

  protected override visitValue(node: ValueNode): void {
    if (node.value === null || node.value === undefined) {
      super.visitValue(node);
      return;
    }

    if (typeof node.value === 'string' && /^\d+$/.test(node.value)) {
      const num = BigInt(node.value);
      if (num > Number.MAX_SAFE_INTEGER || num < Number.MIN_SAFE_INTEGER) {
        super.visitValue(node);
        return;
      }
    }

    super.visitValue(node);
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
        /* c8 ignore start */
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

      for (let i = 0; i < translatedFragments.length; i++) {
        if (i > 0) {
          const param = node.parameters[i - 1];
          if (param) {
            this.visitNode(param);
          }
        }
        const fragment = translatedFragments[i];
        if (fragment) {
          this.append(fragment);
        }
      }
    } else {
      for (let i = 0; i < translatedFragments.length; i++) {
        if (i > 0) {
          const param = node.parameters[i - 1];
          if (param) {
            this.visitNode(param);
          }
        }
        const fragment = translatedFragments[i];
        if (fragment) {
          this.append(fragment);
        }
      }
    }
  }

  protected visitFunctionArgumentList(args: ReadonlyArray<OperationNode>): void {
    this.append('(');
    const lastNode = args[args.length - 1];

    for (const node of args) {
      this.visitNode(node);

      /* c8 ignore start */
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
