import {
  CreateTableNode,
  DeleteQueryNode,
  FunctionNode,
  IdentifierNode,
  MysqlQueryCompiler,
  RawNode,
  SchemableIdentifierNode,
  SetOperationNode,
  TableNode,
  UpdateQueryNode,
  ValueNode,
} from 'kysely';

export class BigQueryQueryCompiler extends MysqlQueryCompiler {
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
        break;
      
      case 'DATE_FORMAT':
        if (node.arguments.length === 2) {
          this.append('FORMAT_TIMESTAMP');
          this.append('(');
          this.visitNode(node.arguments[1]); // format first
          this.append(', ');
          this.visitNode(node.arguments[0]); // then date
          this.append(')');
          return;
        }
        break;
      
      case 'DATE_ADD':
        break;
    }

    super.visitFunction(node);
  }

  protected override visitUpdateQuery(node: UpdateQueryNode): void {
    if (!node.where) {
      // Let BigQuery handle the error for missing WHERE clause
    }
    super.visitUpdateQuery(node);
  }

  protected override visitDeleteQuery(node: DeleteQueryNode): void {
    if (!node.where) {
      // Let BigQuery handle the error for missing WHERE clause
    }
    super.visitDeleteQuery(node);
  }

  protected override visitCreateTable(node: CreateTableNode): void {
    super.visitCreateTable(node);
  }

  protected override visitTable(node: TableNode): void {
    const {table} = node;

    if (table.kind === 'SchemableIdentifierNode') {
      const schemableNode = table as SchemableIdentifierNode;

      if (schemableNode.schema && schemableNode.schema.kind === 'IdentifierNode') {
        const schemaName = (schemableNode.schema as IdentifierNode).name;

        if (schemaName.includes('.')) {
          const [project, dataset] = schemaName.split('.');
          this.visitIdentifier({kind: 'IdentifierNode', name: project} as IdentifierNode);
          this.append('.');
          this.visitIdentifier({kind: 'IdentifierNode', name: dataset} as IdentifierNode);
          this.append('.');
          this.visitIdentifier(schemableNode.identifier);
          return;
        }
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

      fragment = fragment.replace(/\bNOW\s*\(\s*\)/gi, 'CURRENT_TIMESTAMP()');

      translatedFragments[i] = fragment;
    }
    
    if (translatedFragments.some((f) => f.match(/\bDATE_FORMAT\s*\(/i))) {
      for (let i = 0; i < translatedFragments.length; i++) {
        let fragment = translatedFragments[i];

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
        const formatMatch = translatedFragments[1].match(/,\s*'([^']*)'(.*)$/);
        if (formatMatch) {
          this.append(translatedFragments[0]);
          this.append(`'${formatMatch[1]}'`);
          this.append(', ');
          this.visitNode(node.parameters[0]);
          this.append(formatMatch[2]);
          return;
        }
      }

      for (let i = 0; i < translatedFragments.length; i++) {
        if (i > 0 && node.parameters[i - 1]) {
          this.visitNode(node.parameters[i - 1]);
        }
        this.append(translatedFragments[i]);
      }
    } else {
      for (let i = 0; i < translatedFragments.length; i++) {
        if (i > 0 && node.parameters[i - 1]) {
          this.visitNode(node.parameters[i - 1]);
        }
        this.append(translatedFragments[i]);
      }
    }
  }

  protected visitFunctionArgumentList(args: ReadonlyArray<any>): void {
    this.append('(');
    const lastNode = args[args.length - 1];

    for (const node of args) {
      this.visitNode(node);

      if (node !== lastNode) {
        this.append(', ');
      }
    }
    this.append(')');
  }
}